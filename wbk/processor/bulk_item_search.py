"""
Performance-optimized bulk search for very large datasets
Includes caching, indexing, and parallel processing
"""

from typing import List, Dict, Optional, Tuple
import pandas as pd
import json
from RaiseWikibase.dbconnection import DBConnection
from RaiseWikibase.datamodel import entity, label, description

class ItemBulkSearcher:
    
    def __init__(self, cache_size: int = 10000):
        self.connection = DBConnection()
        self.cache = {}
        self.cache_size = cache_size
    
    def _get_from_cache(self, labels: List[str]) -> Tuple[Dict[str, Optional[str]], List[str]]:
        """Get cached results and return uncached labels"""
        cached_results = {}
        uncached_labels = []
        
        for label in labels:
            if label in self.cache:
                cached_results[label] = self.cache[label]
            else:
                uncached_labels.append(label)
        
        return cached_results, uncached_labels
    
    def _update_cache(self, results: Dict[str, Optional[str]]):
        """Update cache with new results"""
        for label, item_id in results.items():
            if len(self.cache) >= self.cache_size:
                # Remove oldest entry (simple FIFO)
                oldest_key = next(iter(self.cache))
                del self.cache[oldest_key]
            
            self.cache[label] = item_id
    
    def find_items_by_labels_optimized(self, labels: List[str], 
                                      use_cache: bool = True,
                                      chunk_size: int = 1000) -> Dict[str, Optional[str]]:
        """
        Optimized bulk search with caching and chunking
        
        Args:
            labels: List of label strings to search for
            use_cache: Whether to use caching
            chunk_size: Number of labels to process at once
            
        Returns:
            Dictionary mapping label -> item_id (or None if not found)
        """
        if not labels:
            return {}
        
        all_results = {}
        
        if use_cache:
            # Get cached results
            cached_results, uncached_labels = self._get_from_cache(labels)
            all_results.update(cached_results)
            labels = uncached_labels
        
        if not labels:
            return all_results
        
        # Process uncached labels in chunks
        for i in range(0, len(labels), chunk_size):
            chunk = labels[i:i + chunk_size]
            chunk_results = self._bulk_find_items_db(chunk)
            
            if use_cache:
                self._update_cache(chunk_results)
            
            all_results.update(chunk_results)
        
        return all_results
    
    def _bulk_find_items_db(self, labels: List[str]) -> Dict[str, Optional[str]]:
        """Database query for bulk finding items"""
        if not labels:
            return {}
        
        # Filter out NaN values and convert to strings, keeping track of original labels
        filtered_labels = []
        
        for label in labels:
            if pd.isna(label) or label is None or str(label).lower() in ['nan', 'none', '']:
                continue
            label_str = str(label)
            # Handle apostrophes - convert to escaped format to match database
            # Database stores apostrophes as \' but search terms have regular '
            label_str = label_str.replace("'", "\\'")
            filtered_labels.append(label_str)
        
        if not filtered_labels:
            return {}
            
        cur = self.connection.conn.cursor()
        placeholders = ','.join(['%s'] * len(filtered_labels))
        
        query = f"""
        SELECT wbx_text as text, wbit_item_id as id 
        FROM wbt_item_terms 
        LEFT JOIN wbt_term_in_lang ON wbit_term_in_lang_id = wbtl_id 
        LEFT JOIN wbt_text_in_lang ON wbtl_text_in_lang_id = wbxl_id 
        LEFT JOIN wbt_text ON wbxl_text_id = wbx_id 
        WHERE wbtl_type_id = 1 AND wbx_text IN ({placeholders})
        """
        
        try:
            cur.execute(query, filtered_labels)
            results = cur.fetchall()
            
            # Build results dictionary
            label_to_id = {}
            
            # Add results from database
            for text, item_id in results:
                # Convert bytes to string if necessary
                if isinstance(text, bytes):
                    text = text.decode('utf-8')
                
                text = text.replace("\\'", "'")
                label_to_id[text] = f'Q{item_id}'
            
        except Exception as e:
            print(f"Error in bulk search: {e}")
            label_to_id = {}
        finally:
            cur.close()

            
        return label_to_id
    
    
    def find_items_by_labels_with_data(
        self,
        labels: List[str],
        language: str = 'en',
        use_cache: bool = True,
        chunk_size: int = 1000
    ) -> Dict[str, dict]:
        """Find items by labels and return full entity data.
        
        Uses a single atomic database transaction to fetch both QIDs and item data.
        
        Args:
            labels: List of label strings to search for
            language: Language code for labels/descriptions (default: 'en')
            use_cache: Whether to use caching (currently not used for full data)
            chunk_size: Number of labels to process at once
            
        Returns:
            Dictionary mapping label -> entity dict (RaiseWikibase format)
        """
        if not labels:
            return {}
        
        items_by_label = {}
        
        # Process labels in chunks for better performance
        for i in range(0, len(labels), chunk_size):
            chunk = labels[i:i + chunk_size]
            chunk_results = self._bulk_find_items_with_data_db(
                chunk, language=language
            )
            items_by_label.update(chunk_results)
        
        # Handle labels that weren't found
        for label_str in labels:
            if label_str not in items_by_label:
                items_by_label[label_str] = self._create_empty_item(
                    item_qid=None, item_label=label_str, language=language
                )
        
        return items_by_label
    
    def _bulk_find_items_with_data_db(
        self,
        labels: List[str],
        language: str = 'en'
    ) -> Dict[str, dict]:
        """Bulk find items with full data in a single atomic query.
        
        Args:
            labels: List of label strings to search for
            language: Language code for labels/descriptions
            
        Returns:
            Dictionary mapping label -> entity dict
        """
        if not labels:
            return {}
        
        # Filter out NaN values and convert to strings
        filtered_labels = []
        for label in labels:
            if pd.isna(label) or label is None or str(label).lower() in ['nan', 'none', '']:
                continue
            label_str = str(label)
            # Handle apostrophes - convert to escaped format to match database
            label_str = label_str.replace("'", "\\'")
            filtered_labels.append(label_str)
        
        if not filtered_labels:
            return {}
        
        cur = self.connection.conn.cursor()
        placeholders = ','.join(['%s'] * len(filtered_labels))
        
        # Single atomic query that gets labels, QIDs, and item JSON data
        # Join order: wbt_item_terms -> page (by QID) -> text (by page_latest)
        # Note: page.page_title is stored as VARCHAR, so we need to ensure
        # CONCAT produces a string that matches exactly
        query = f"""
        SELECT 
            wbx_text as label,
            CONCAT('Q', wbit_item_id) as item_qid,
            text.old_text as item_json
        FROM wbt_item_terms 
        INNER JOIN wbt_term_in_lang ON wbit_term_in_lang_id = wbtl_id 
        INNER JOIN wbt_text_in_lang ON wbtl_text_in_lang_id = wbxl_id 
        INNER JOIN wbt_text ON wbxl_text_id = wbx_id 
        LEFT JOIN page ON CAST(page.page_title AS CHAR) = CAST(CONCAT('Q', wbit_item_id) AS CHAR)
        LEFT JOIN text ON text.old_id = page.page_latest
        WHERE wbtl_type_id = 1 AND wbx_text IN ({placeholders})
        """
        
        items_by_label = {}
        
        try:
            cur.execute(query, filtered_labels)
            results = cur.fetchall()
            
            for label_text, item_qid, item_json_text in results:
                # Convert bytes to string if necessary
                if isinstance(label_text, bytes):
                    label_text = label_text.decode('utf-8')
                label_text = label_text.replace("\\'", "'")
                
                if not item_qid:
                    # Item not found, create empty entity
                    items_by_label[label_text] = self._create_empty_item(
                        item_qid=None, item_label=label_text, language=language
                    )
                    continue
                
                # Parse the JSON to get the item data
                if item_json_text:
                    try:
                        if isinstance(item_json_text, bytes):
                            item_json_text = item_json_text.decode('utf-8')
                        item_json = json.loads(item_json_text)
                        
                        # Extract claims, labels, descriptions
                        claims_dict = item_json.get('claims', {})
                        labels_dict = item_json.get('labels', {})
                        descriptions_dict = item_json.get('descriptions', {})
                        
                        item_entity = entity(
                            labels=labels_dict if labels_dict else {},
                            aliases={},
                            descriptions=descriptions_dict if descriptions_dict else {},
                            claims=claims_dict,
                            etype='item'
                        )
                        item_entity['id'] = item_qid
                        items_by_label[label_text] = item_entity
                    except (json.JSONDecodeError, Exception) as e:
                        print(
                            f"Warning: Could not parse item JSON for {item_qid}: {e}. "
                            f"Creating empty item structure."
                        )
                        items_by_label[label_text] = self._create_empty_item(
                            item_qid=item_qid, item_label=label_text, language=language
                        )
                else:
                    # No JSON data found, create empty entity
                    items_by_label[label_text] = self._create_empty_item(
                        item_qid=item_qid, item_label=label_text, language=language
                    )
                    
        except Exception as e:
            print(f"Error in bulk search with data: {e}")
        finally:
            cur.close()
        
        return items_by_label
    
   
    def _create_empty_item(
        self,
        item_qid: Optional[str],
        item_label: str,
        language: str = 'en'
    ) -> dict:
        """Create an empty item entity structure.
        
        Args:
            item_qid: Item QID (can be None if not found)
            item_label: Item label
            language: Language code for labels
            
        Returns:
            Empty entity dict in RaiseWikibase format
        """
        empty_item = entity(
            labels=label(language, item_label),
            aliases={},
            descriptions={},
            claims={},
            etype='item'
        )
        if item_qid:
            empty_item['id'] = item_qid
        return empty_item
    
    def clear_cache(self):
        """Clear the cache"""
        self.cache.clear()
    
    def close(self):
        """Close database connection"""
        self.connection.conn.close()

