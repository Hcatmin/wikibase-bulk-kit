"""
Performance-optimized bulk search for very large datasets
Includes caching, indexing, and parallel processing
"""

from typing import List, Dict, Optional, Tuple, Any
import pandas as pd
import json
from RaiseWikibase.dbconnection import DBConnection
from RaiseWikibase.datamodel import entity, label, description

class ItemBulkSearcher:
    
    def __init__(self, cache_size: int = 10000):
        self.connection = DBConnection()
        self.cache = {}
        self.cache_size = cache_size

    def __enter__(self, cache_size: int = 10000):
        self.connection = DBConnection()
        self.cache = {}
        self.cache_size = cache_size
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _normalize_unique_value(
        self,
        value: Any | None,
        property_datatype: Optional[str] = None,
    ) -> Optional[str]:
        """Normalize unique-key values for comparison (datatype-aware)."""
        if value is None:
            return None

        normalized: Any = value
        if isinstance(normalized, dict):
            normalized = (
                normalized.get("amount")
                or normalized.get("value")
                or normalized.get("text")
                or normalized.get("id")
                or normalized
            )

        if property_datatype == "quantity":
            try:
                if isinstance(normalized, str) and normalized.startswith("+"):
                    normalized = normalized[1:]
                numeric = float(str(normalized))
                if numeric.is_integer():
                    normalized = str(int(numeric))
                else:
                    normalized = str(numeric)
            except Exception:
                normalized = str(normalized)
        else:
            normalized = str(normalized)

        normalized = normalized.strip()
        return normalized or None
    
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

    def find_qids(
        self,
        keys: List[Tuple[str, Optional[str]]],
        chunk_size: int = 1000,
    ) -> Dict[Tuple[str, Optional[str]], Optional[str]]:
        """
        Find item QIDs by (label, description) primary keys.

        Args:
            keys: List of (label, description) tuples. If description is None
                  or empty, falls back to label-only lookup.
            chunk_size: Number of keys to process per database query.

        Returns:
            Dictionary mapping (label, description) -> QID (or None).
        """
        if not keys:
            return {}

        label_desc_pairs: List[Tuple[str, str]] = []
        label_only: List[str] = []

        for label_value, description_value in keys:
            if label_value is None:
                continue
            label_str = str(label_value).strip()
            if not label_str:
                continue

            if description_value is None or str(description_value).strip() == "":
                label_only.append(label_str)
            else:
                desc_str = str(description_value).strip()
                label_desc_pairs.append((label_str, desc_str))

        # Deduplicate while preserving order
        label_desc_pairs = list(dict.fromkeys(label_desc_pairs))
        label_only = list(dict.fromkeys(label_only))

        results: Dict[Tuple[str, Optional[str]], Optional[str]] = {}

        # First, resolve label+description pairs
        for i in range(0, len(label_desc_pairs), chunk_size):
            chunk = label_desc_pairs[i : i + chunk_size]
            if not chunk:
                continue
            chunk_results = self._bulk_find_qids_by_label_and_description_db(chunk)
            results.update(chunk_results)

        # Fallback: label-only lookups for missing descriptions
        if label_only:
            label_results = self.find_items_by_labels_optimized(label_only)
            for label_text, qid in label_results.items():
                results[(label_text, None)] = qid

        return results

    def _bulk_find_qids_by_label_and_description_db(
        self,
        pairs: List[Tuple[str, str]],
    ) -> Dict[Tuple[str, Optional[str]], Optional[str]]:
        """
        Database query for bulk finding items by (label, description).

        Returns:
            Dict mapping (label, description) -> QID.
        """
        if not pairs:
            return {}

        sanitized_pairs: List[Tuple[str, str]] = []
        for label_value, description_value in pairs:
            if pd.isna(label_value) or pd.isna(description_value):
                continue
            label_str = str(label_value).replace("'", "\\'")
            description_str = str(description_value).replace("'", "\\'")
            sanitized_pairs.append((label_str, description_str))

        if not sanitized_pairs:
            return {}

        placeholders = ",".join(["(%s, %s)"] * len(sanitized_pairs))

        cur = self.connection.conn.cursor()
        query = f"""
        SELECT 
            labels.wbx_text as label_text,
            descriptions.wbx_text as description_text,
            label_terms.wbit_item_id as id
        FROM wbt_item_terms AS label_terms
        INNER JOIN wbt_term_in_lang AS label_lang
            ON label_terms.wbit_term_in_lang_id = label_lang.wbtl_id
        INNER JOIN wbt_text_in_lang AS label_text_lang
            ON label_lang.wbtl_text_in_lang_id = label_text_lang.wbxl_id
        INNER JOIN wbt_text AS labels
            ON label_text_lang.wbxl_text_id = labels.wbx_id
        INNER JOIN wbt_item_terms AS desc_terms
            ON desc_terms.wbit_item_id = label_terms.wbit_item_id
        INNER JOIN wbt_term_in_lang AS desc_lang
            ON desc_terms.wbit_term_in_lang_id = desc_lang.wbtl_id
        INNER JOIN wbt_text_in_lang AS desc_text_lang
            ON desc_lang.wbtl_text_in_lang_id = desc_text_lang.wbxl_id
        INNER JOIN wbt_text AS descriptions
            ON desc_text_lang.wbxl_text_id = descriptions.wbx_id
        WHERE label_lang.wbtl_type_id = 1
          AND desc_lang.wbtl_type_id = 2
          AND (labels.wbx_text, descriptions.wbx_text) IN ({placeholders})
        """

        params: List[str] = []
        for label_text, description_text in sanitized_pairs:
            params.extend([label_text, description_text])

        key_to_qid: Dict[Tuple[str, Optional[str]], Optional[str]] = {}

        try:
            cur.execute(query, params)
            results = cur.fetchall()

            for label_text, description_text, item_id in results:
                if isinstance(label_text, bytes):
                    label_text = label_text.decode("utf-8")
                if isinstance(description_text, bytes):
                    description_text = description_text.decode("utf-8")

                label_text = label_text.replace("\\'", "'")
                description_text = description_text.replace("\\'", "'")
                key_to_qid[(label_text, description_text)] = f"Q{item_id}"
        except Exception as e:
            print(f"Error in label/description bulk search: {e}")
        finally:
            cur.close()

        return key_to_qid
    
    
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
    
    def find_items_by_qids(
        self,
        qids: List[Any],
        language: str = "en",
        chunk_size: int = 1000,
    ) -> Dict[str, dict]:
        """Find items by QIDs and return full entity data."""
        if not qids:
            return {}

        normalized: List[str] = []
        for qid in qids:
            if pd.isna(qid) or qid is None:
                continue
            qid_str = str(qid).strip()
            if not qid_str:
                continue
            if not qid_str.upper().startswith("Q"):
                qid_str = f"Q{qid_str}"
            else:
                qid_str = f"Q{qid_str[1:]}" if qid_str.startswith("q") else qid_str
            normalized.append(qid_str)

        normalized = list(dict.fromkeys(normalized))
        if not normalized:
            return {}

        items_by_qid: Dict[str, dict] = {}
        for i in range(0, len(normalized), chunk_size):
            chunk = normalized[i : i + chunk_size]
            chunk_items = self._bulk_find_items_with_data_by_qid_db(
                chunk,
                language=language,
            )
            items_by_qid.update(chunk_items)

        for qid in normalized:
            if qid not in items_by_qid:
                items_by_qid[qid] = self._create_empty_item(
                    item_qid=qid,
                    item_label=qid,
                    language=language,
                )

        return items_by_qid
    
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

    def find_items(
        self,
        keys: List[Tuple[str, Optional[str]]],
        language: str = "en",
        chunk_size: int = 1000,
    ) -> Dict[Tuple[str, Optional[str]], dict]:
        """
        Find items by (label, description) primary keys.

        Args:
            keys: List of (label, description) tuples. If description is None
                  or empty, falls back to label-only lookup.
            language: Language code for labels/descriptions.
            chunk_size: Number of keys to process per database query.

        Returns:
            Dictionary mapping (label, description) -> item entity dict.
        """
        if not keys:
            return {}

        label_desc_pairs: List[Tuple[str, str]] = []
        label_only: List[str] = []

        for label_value, description_value in keys:
            if label_value is None:
                continue
            label_str = str(label_value).strip()
            if not label_str:
                continue

            if description_value is None or str(description_value).strip() == "":
                label_only.append(label_str)
            else:
                desc_str = str(description_value).strip()
                label_desc_pairs.append((label_str, desc_str))

        label_desc_pairs = list(dict.fromkeys(label_desc_pairs))
        label_only = list(dict.fromkeys(label_only))

        items_by_key: Dict[Tuple[str, Optional[str]], dict] = {}

        # Resolve pairs with both label and description
        for i in range(0, len(label_desc_pairs), chunk_size):
            chunk = label_desc_pairs[i : i + chunk_size]
            if not chunk:
                continue
            chunk_results = self._bulk_find_items_with_data_by_label_and_description_db(
                chunk,
                language=language,
            )
            items_by_key.update(chunk_results)

        # Fill in missing pairs with empty items
        for label_text, description_text in label_desc_pairs:
            key = (label_text, description_text)
            if key not in items_by_key:
                items_by_key[key] = self._create_empty_item(
                    item_qid=None,
                    item_label=label_text,
                    language=language,
                )

        # Fallback: label-only lookups
        if label_only:
            label_items = self.find_items_by_labels_with_data(
                label_only,
                language=language,
                chunk_size=chunk_size,
            )
            for label_text, item in label_items.items():
                items_by_key[(label_text, None)] = item

        return items_by_key

    def _bulk_find_items_with_data_by_label_and_description_db(
        self,
        pairs: List[Tuple[str, str]],
        language: str = "en",
    ) -> Dict[Tuple[str, Optional[str]], dict]:
        """
        Bulk find items with full data keyed by (label, description).

        Returns:
            Dict mapping (label, description) -> item entity dict.
        """
        if not pairs:
            return {}

        sanitized_pairs: List[Tuple[str, str]] = []
        for label_value, description_value in pairs:
            if pd.isna(label_value) or pd.isna(description_value):
                continue
            label_str = str(label_value).replace("'", "\\'")
            description_str = str(description_value).replace("'", "\\'")
            sanitized_pairs.append((label_str, description_str))

        if not sanitized_pairs:
            return {}

        placeholders = ",".join(["(%s, %s)"] * len(sanitized_pairs))
        cur = self.connection.conn.cursor()

        query = f"""
        SELECT 
            labels.wbx_text as label_text,
            descriptions.wbx_text as description_text,
            CONCAT('Q', label_terms.wbit_item_id) as item_qid,
            text.old_text as item_json
        FROM wbt_item_terms AS label_terms
        INNER JOIN wbt_term_in_lang AS label_lang
            ON label_terms.wbit_term_in_lang_id = label_lang.wbtl_id
        INNER JOIN wbt_text_in_lang AS label_text_lang
            ON label_lang.wbtl_text_in_lang_id = label_text_lang.wbxl_id
        INNER JOIN wbt_text AS labels
            ON label_text_lang.wbxl_text_id = labels.wbx_id
        INNER JOIN wbt_item_terms AS desc_terms
            ON desc_terms.wbit_item_id = label_terms.wbit_item_id
        INNER JOIN wbt_term_in_lang AS desc_lang
            ON desc_terms.wbit_term_in_lang_id = desc_lang.wbtl_id
        INNER JOIN wbt_text_in_lang AS desc_text_lang
            ON desc_lang.wbtl_text_in_lang_id = desc_text_lang.wbxl_id
        INNER JOIN wbt_text AS descriptions
            ON desc_text_lang.wbxl_text_id = descriptions.wbx_id
        LEFT JOIN page
            ON CAST(page.page_title AS CHAR) = CAST(CONCAT('Q', label_terms.wbit_item_id) AS CHAR)
        LEFT JOIN text
            ON text.old_id = page.page_latest
        WHERE label_lang.wbtl_type_id = 1
          AND desc_lang.wbtl_type_id = 2
          AND (labels.wbx_text, descriptions.wbx_text) IN ({placeholders})
        """

        params: List[str] = []
        for label_text, description_text in sanitized_pairs:
            params.extend([label_text, description_text])

        items_by_key: Dict[Tuple[str, Optional[str]], dict] = {}

        try:
            cur.execute(query, params)
            results = cur.fetchall()

            for (
                label_text,
                description_text,
                item_qid,
                item_json_text,
            ) in results:
                if isinstance(label_text, bytes):
                    label_text = label_text.decode("utf-8")
                if isinstance(description_text, bytes):
                    description_text = description_text.decode("utf-8")

                label_text = label_text.replace("\\'", "'")
                description_text = description_text.replace("\\'", "'")

                key = (label_text, description_text)

                if not item_qid:
                    items_by_key[key] = self._create_empty_item(
                        item_qid=None,
                        item_label=label_text,
                        language=language,
                    )
                    continue

                if item_json_text:
                    try:
                        if isinstance(item_json_text, bytes):
                            item_json_text = item_json_text.decode("utf-8")
                        item_json = json.loads(item_json_text)

                        claims_dict = item_json.get("claims", {})
                        labels_dict = item_json.get("labels", {})
                        descriptions_dict = item_json.get("descriptions", {})

                        item_entity = entity(
                            labels=labels_dict if labels_dict else {},
                            aliases={},
                            descriptions=descriptions_dict if descriptions_dict else {},
                            claims=claims_dict,
                            etype="item",
                        )
                        item_entity["id"] = item_qid
                        items_by_key[key] = item_entity
                    except (json.JSONDecodeError, Exception) as e:
                        print(
                            f"Warning: Could not parse item JSON for {item_qid}: {e}. "
                            f"Creating empty item structure."
                        )
                        items_by_key[key] = self._create_empty_item(
                            item_qid=item_qid,
                            item_label=label_text,
                            language=language,
                        )
                else:
                    items_by_key[key] = self._create_empty_item(
                        item_qid=item_qid,
                        item_label=label_text,
                        language=language,
                    )
        except Exception as e:
            print(f"Error in label/description data bulk search: {e}")
        finally:
            cur.close()

        return items_by_key
    
    def _bulk_find_items_with_data_by_qid_db(
        self,
        qids: List[str],
        language: str = "en",
    ) -> Dict[str, dict]:
        """Bulk find items with full data keyed by QID."""
        if not qids:
            return {}

        placeholders = ",".join(["%s"] * len(qids))
        cur = self.connection.conn.cursor()

        query = f"""
        SELECT 
            page.page_title as qid,
            text.old_text as item_json
        FROM page
        LEFT JOIN text
            ON text.old_id = page.page_latest
        WHERE page.page_title IN ({placeholders})
        """

        items_by_qid: Dict[str, dict] = {}

        try:
            cur.execute(query, qids)
            results = cur.fetchall()

            for qid_text, item_json_text in results:
                if isinstance(qid_text, bytes):
                    qid_text = qid_text.decode("utf-8")

                if item_json_text:
                    try:
                        if isinstance(item_json_text, bytes):
                            item_json_text = item_json_text.decode("utf-8")
                        item_json = json.loads(item_json_text)

                        claims_dict = item_json.get("claims", {})
                        labels_dict = item_json.get("labels", {})
                        descriptions_dict = item_json.get("descriptions", {})

                        item_entity = entity(
                            labels=labels_dict if labels_dict else {},
                            aliases={},
                            descriptions=descriptions_dict if descriptions_dict else {},
                            claims=claims_dict if claims_dict else {},
                            etype="item",
                        )
                        item_entity["id"] = qid_text
                        items_by_qid[qid_text] = item_entity
                    except (json.JSONDecodeError, Exception) as e:
                        print(
                            f"Warning: Could not parse item JSON for {qid_text}: {e}. "
                            f"Creating empty item structure."
                        )
                        items_by_qid[qid_text] = self._create_empty_item(
                            item_qid=qid_text,
                            item_label=qid_text,
                            language=language,
                        )
                else:
                    items_by_qid[qid_text] = self._create_empty_item(
                        item_qid=qid_text,
                        item_label=qid_text,
                        language=language,
                    )
        except Exception as e:
            print(f"Error in QID data bulk search: {e}")
        finally:
            cur.close()

        return items_by_qid
    

    def _extract_claim_values(
        self,
        item_json: dict,
        property_id: str,
        property_datatype: Optional[str],
    ) -> List[str]:
        claims = item_json.get("claims") or {}
        if property_id not in claims:
            return []
        values: List[str] = []
        for claim_obj in claims.get(property_id, []):
            mainsnak = claim_obj.get("mainsnak", {})
            datavalue = mainsnak.get("datavalue", {})
            raw_value = datavalue.get("value")
            normalized = self._normalize_unique_value(raw_value, property_datatype)
            if normalized is not None:
                values.append(normalized)
        return values

    def _fetch_items_with_data(
        self,
        labels: List[str],
        language: str = "en",
    ) -> List[Tuple[str, Optional[str], Any]]:
        """Fetch items (label, qid, json) for a list of labels without collapsing duplicates."""
        if not labels:
            return []

        filtered_labels = []
        for label_value in labels:
            if pd.isna(label_value) or label_value is None or str(label_value).lower() in ["nan", "none", ""]:
                continue
            label_str = str(label_value).replace("'", "\\'")
            filtered_labels.append(label_str)

        if not filtered_labels:
            return []

        placeholders = ",".join(["%s"] * len(filtered_labels))
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

        cur = self.connection.conn.cursor()
        rows: List[Tuple[str, Optional[str], Any]] = []
        try:
            cur.execute(query, filtered_labels)
            results = cur.fetchall()
            for label_text, item_qid, item_json_text in results:
                if isinstance(label_text, bytes):
                    label_text = label_text.decode("utf-8")
                label_text = label_text.replace("\\'", "'")
                rows.append((label_text, item_qid, item_json_text))
        except Exception as e:
            print(f"Error in unique-key bulk search: {e}")
        finally:
            cur.close()
        return rows

    def find_qids_by_unique_key(
        self,
        keys: List[Tuple[str, Optional[str]]],
        property_id: str,
        property_datatype: Optional[str] = None,
        language: str = "en",
    ) -> Dict[Tuple[str, Optional[str]], Optional[str]]:
        """Find QIDs using (label, unique value) pairs scoped by a property."""
        if not keys:
            return {}

        label_to_values: Dict[str, List[Optional[str]]] = {}
        for label_value, unique_val in keys:
            if label_value is None:
                continue
            label_str = str(label_value).strip()
            if not label_str:
                continue
            normalized_value = self._normalize_unique_value(unique_val, property_datatype)
            label_to_values.setdefault(label_str, []).append(normalized_value)

        if not label_to_values:
            return {}

        rows = self._fetch_items_with_data(list(label_to_values.keys()), language=language)
        results: Dict[Tuple[str, Optional[str]], Optional[str]] = {}

        for label_text, item_qid, item_json_text in rows:
            if not item_qid or not item_json_text:
                continue
            try:
                if isinstance(item_json_text, bytes):
                    item_json_text = item_json_text.decode("utf-8")
                item_json = json.loads(item_json_text)
            except Exception:
                continue

            claim_values = self._extract_claim_values(item_json, property_id, property_datatype)
            expected_values = label_to_values.get(label_text, [])
            for expected in expected_values:
                if expected is None:
                    continue
                if expected in claim_values and (label_text, expected) not in results:
                    results[(label_text, expected)] = item_qid
        return results

    def find_items_by_unique_key(
        self,
        keys: List[Tuple[str, Optional[str]]],
        property_id: str,
        property_datatype: Optional[str] = None,
        language: str = "en",
    ) -> Dict[Tuple[str, Optional[str]], dict]:
        """Find items by (label, unique_value) pairs using the provided property as unique key."""
        if not keys:
            return {}

        label_to_values: Dict[str, List[Optional[str]]] = {}
        for label_value, unique_val in keys:
            if label_value is None:
                continue
            label_str = str(label_value).strip()
            if not label_str:
                continue
            normalized_value = self._normalize_unique_value(unique_val, property_datatype)
            label_to_values.setdefault(label_str, []).append(normalized_value)

        if not label_to_values:
            return {}

        rows = self._fetch_items_with_data(list(label_to_values.keys()), language=language)
        items_by_key: Dict[Tuple[str, Optional[str]], dict] = {}

        for label_text, item_qid, item_json_text in rows:
            if not item_json_text:
                continue
            try:
                if isinstance(item_json_text, bytes):
                    item_json_text = item_json_text.decode("utf-8")
                item_json = json.loads(item_json_text)
            except Exception:
                item_json = {}

            claim_values = self._extract_claim_values(item_json, property_id, property_datatype)
            expected_values = label_to_values.get(label_text, [])
            for expected in expected_values:
                if expected is None:
                    continue
                if expected not in claim_values:
                    continue

                # Build entity structure\n                claims_dict = item_json.get(\"claims\", {}) if item_json else {}\n                labels_dict = item_json.get(\"labels\", {}) if item_json else {}\n                descriptions_dict = item_json.get(\"descriptions\", {}) if item_json else {}\n\n                if not item_qid:\n                    item_entity = self._create_empty_item(\n                        item_qid=None, item_label=label_text, language=language\n                    )\n                else:\n                    item_entity = entity(\n                        labels=labels_dict if labels_dict else {},\n                        aliases={},\n                        descriptions=descriptions_dict if descriptions_dict else {},\n                        claims=claims_dict if claims_dict else {},\n                        etype=\"item\",\n                    )\n                    item_entity[\"id\"] = item_qid\n\n                items_by_key[(label_text, expected)] = item_entity\n\n        return items_by_key\n-   
    
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
