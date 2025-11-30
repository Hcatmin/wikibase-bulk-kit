from typing import List, Optional, Dict, Any, Tuple
from wbk.backend.interface import BackendStrategy
from wbk.schema.models import PropertySchema, ItemSchema
from wbk.processor.bulk_item_search import ItemBulkSearcher
from RaiseWikibase.raiser import batch
from RaiseWikibase.datamodel import entity, label, description

class RaiseWikibaseBackend(BackendStrategy):
    """Backend strategy using RaiseWikibase for optimized bulk operations."""

    def __init__(self):
        pass

    def find_property_by_label(self, label: str, language: str) -> Optional[str]:
        # Not implemented efficiently yet, fallback or TODO
        return None

    def find_item_by_label(self, label: str, language: str) -> Optional[str]:
        with ItemBulkSearcher() as searcher:
            results = searcher.find_items_by_labels_optimized([label])
            return results.get(label)

    def find_item_by_label_and_description(self, label: str, description: str, language: str) -> Optional[str]:
        with ItemBulkSearcher() as searcher:
            results = searcher.find_qids([(label, description)])
            return results.get((label, description))

    def find_item_by_expression(self, expression: str, language: str) -> Optional[str]:
        # TODO: Implement expression parsing and search
        return None

    def create_property(self, property_schema: PropertySchema, language: str) -> Optional[str]:
        # Not supported by RaiseWikibase batch efficiently for single prop?
        return None

    def update_property(self, property_schema: PropertySchema, language: str) -> bool:
        return False

    def create_item(self, item_schema: ItemSchema, language: str) -> Optional[str]:
        # Use batch with single item
        item_dict = entity(
            labels=label(language, item_schema.label),
            descriptions=description(language, item_schema.description) if item_schema.description else {},
            etype='item'
        )
        # Add aliases, claims... (simplified for now)
        
        results = batch(content_model='wikibase-item', texts=[item_dict], new=True)
        if results and len(results) > 0:
            return results[0].get('id')
        return None

    def update_item(self, item_schema: ItemSchema, language: str) -> bool:
        # Use batch with single item
        # We need the existing item data or just the ID?
        # RaiseWikibase batch expects full item structure usually?
        # Or at least ID.
        if not item_schema.id:
            return False
            
        item_dict = entity(
            labels=label(language, item_schema.label),
            descriptions=description(language, item_schema.description) if item_schema.description else {},
            etype='item'
        )
        item_dict['id'] = item_schema.id
        
        results = batch(content_model='wikibase-item', texts=[item_dict], new=False)
        return bool(results)

    def find_qids(self, keys: List[dict], language: str) -> dict:
        """
        Bulk find QIDs.
        Adapts to ItemBulkSearcher.
        """
        label_desc_pairs = []
        key_map = {} # Map (label, desc) -> [indices]
        
        for i, key in enumerate(keys):
            if 'unique_key' in key:
                # TODO: Implement unique key search
                # For now, we can't search by unique key with ItemBulkSearcher
                continue
            
            if 'label' in key:
                lbl = key['label']
                desc = key.get('description')
                pair = (lbl, desc)
                label_desc_pairs.append(pair)
                if pair not in key_map:
                    key_map[pair] = []
                key_map[pair].append(i)
        
        results = {}
        if label_desc_pairs:
            with ItemBulkSearcher() as searcher:
                # find_qids takes list of tuples
                found = searcher.find_qids(label_desc_pairs)
                
                for pair, qid in found.items():
                    if qid:
                        for idx in key_map.get(pair, []):
                            results[idx] = qid
                            
        return results

    def create_items(self, items: List[dict], language: str) -> List[str]:
        if not items:
            return []
        
        # RaiseWikibase batch returns list of created items (dicts)
        created_items = batch(content_model='wikibase-item', texts=items, new=True)
        
        qids = []
        for item in created_items:
            qids.append(item.get('id'))
            
        return qids

    def update_items(self, items: List[dict], language: str) -> List[bool]:
        if not items:
            return []
            
        # RaiseWikibase batch returns list of updated items
        updated_items = batch(content_model='wikibase-item', texts=items, new=False)
        
        # Assume success if returned
        return [True] * len(updated_items)
