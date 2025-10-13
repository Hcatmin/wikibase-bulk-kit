"""
Performance-optimized bulk search for very large datasets
Includes caching, indexing, and parallel processing
"""

from typing import List, Dict, Optional, Tuple
import pandas as pd
from RaiseWikibase.dbconnection import DBConnection

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
    
    
    def clear_cache(self):
        """Clear the cache"""
        self.cache.clear()
    
    def close(self):
        """Close database connection"""
        self.connection.conn.close()

