from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from RaiseWikibase.datamodel import entity, label, description
from RaiseWikibase.dbconnection import DBConnection
from RaiseWikibase.raiser import batch

from wbk.backend.interface import BackendStrategy
from wbk.schema.models import ItemSchema, PropertySchema


def _decode_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


class RaiseWikibaseBackend(BackendStrategy):
    """Backend strategy using RaiseWikibase for optimized bulk operations."""

    def __init__(self):
        pass

    @contextmanager
    def _db_cursor(self):
        connection = DBConnection()
        cursor = connection.conn.cursor()
        try:
            yield cursor
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            connection.conn.close()

    @staticmethod
    def _normalize_label(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        lower = text.lower()
        if lower in {"nan", "none"}:
            return None
        return text

    @staticmethod
    def _escape_label(value: str) -> str:
        return value.replace("'", "\\'")

    def _normalize_unique_value(
        self,
        value: Any | None,
        property_datatype: Optional[str] = None,
    ) -> Optional[str]:
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
                normalized = str(int(numeric)) if numeric.is_integer() else str(numeric)
            except Exception:
                normalized = str(normalized)
        else:
            normalized = str(normalized)

        normalized = normalized.strip()
        return normalized or None

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

    def _create_empty_item(
        self,
        item_qid: Optional[str],
        item_label: str,
        language: str,
    ) -> dict:
        empty_item = entity(
            labels=label(language, item_label),
            aliases={},
            descriptions={},
            claims={},
            etype="item",
        )
        if item_qid:
            empty_item["id"] = item_qid
        return empty_item

    def find_property_by_label(self, label: str, language: str) -> Optional[str]:
        # Not implemented efficiently yet, fallback or TODO
        return None

    def find_item_by_label(self, label: str, language: str) -> Optional[str]:
        return self.get_qid_by_label(label)

    def find_item_by_label_and_description(
        self, label: str, description: str, language: str
    ) -> Optional[str]:
        return self.get_qid_by_label_and_description(label, description)

    def find_item_by_expression(self, expression: str, language: str) -> Optional[str]:
        # TODO: Implement expression parsing and search
        return None

    def create_property(self, property_schema: PropertySchema, language: str) -> Optional[str]:
        # Not supported by RaiseWikibase batch efficiently for single prop?
        return None

    def update_property(self, property_schema: PropertySchema, language: str) -> bool:
        return False

    def create_item(self, item_schema: ItemSchema, language: str) -> Optional[str]:
        item_dict = entity(
            labels=label(language, item_schema.label),
            descriptions=description(language, item_schema.description)
            if item_schema.description
            else {},
            etype="item",
        )
        results = batch(content_model="wikibase-item", texts=[item_dict], new=True)
        if results and len(results) > 0:
            return results[0].get("id")
        return None

    def update_item(self, item_schema: ItemSchema, language: str) -> bool:
        if not item_schema.id:
            return False

        item_dict = entity(
            labels=label(language, item_schema.label),
            descriptions=description(language, item_schema.description)
            if item_schema.description
            else {},
            etype="item",
        )
        item_dict["id"] = item_schema.id

        results = batch(content_model="wikibase-item", texts=[item_dict], new=False)
        return bool(results)

    def find_qids(self, keys: List[dict], language: str) -> dict:
        """Bulk find QIDs from key dictionaries (label/description/unique_key)."""
        idx_to_qid: Dict[int, Optional[str]] = {}
        label_desc_pairs: List[Tuple[str, str]] = []
        label_only: List[str] = []
        pair_indices: Dict[Tuple[str, str], List[int]] = {}

        for i, key in enumerate(keys):
            label_value = key.get("label")
            if label_value is None:
                idx_to_qid[i] = None
                continue
            label_norm = self._normalize_label(label_value)
            if not label_norm:
                idx_to_qid[i] = None
                continue

            desc_value = key.get("description")
            if desc_value:
                desc_norm = self._normalize_label(desc_value)
                if desc_norm:
                    pair = (label_norm, desc_norm)
                    label_desc_pairs.append(pair)
                    pair_indices.setdefault(pair, []).append(i)
                    continue
            label_only.append(label_norm)
            pair_indices.setdefault((label_norm, None), []).append(i)

        if label_desc_pairs:
            found_pairs = self._find_qids_by_label_and_description(label_desc_pairs)
            for pair, qid in found_pairs.items():
                for idx in pair_indices.get(pair, []):
                    idx_to_qid[idx] = qid

        if label_only:
            found_labels = self.find_items_by_labels_optimized(label_only)
            for label, qid in found_labels.items():
                for idx in pair_indices.get((label, None), []):
                    idx_to_qid[idx] = qid

        return idx_to_qid

    def create_items(self, items: List[dict], language: str) -> List[str]:
        if not items:
            return []

        created_items = batch(content_model="wikibase-item", texts=items, new=True)
        return [item.get("id") for item in created_items]

    def update_items(self, items: List[dict], language: str) -> List[bool]:
        if not items:
            return []

        updated_items = batch(content_model="wikibase-item", texts=items, new=False)
        return [True] * len(updated_items)

    def get_qid_by_label(self, label: str) -> Optional[str]:
        label_norm = self._normalize_label(label)
        if not label_norm:
            return None
        with self._db_cursor() as cursor:
            return self._select_qid_by_label(cursor, self._escape_label(label_norm))

    def get_qid_by_label_and_description(
        self,
        label: str,
        description: str,
    ) -> Optional[str]:
        label_norm = self._normalize_label(label)
        desc_norm = self._normalize_label(description)
        if not label_norm or not desc_norm:
            return None
        with self._db_cursor() as cursor:
            return self._select_qid_by_label_and_description(
                cursor,
                self._escape_label(label_norm),
                self._escape_label(desc_norm),
            )

    def get_item_by_label(
        self,
        label: str,
        language: str = "en",
    ) -> Optional[dict]:
        qid = self.get_qid_by_label(label)
        if not qid:
            return None
        return self._load_item_by_qid(qid, language)

    def get_item_by_label_and_description(
        self,
        label: str,
        description: str,
        language: str = "en",
    ) -> Optional[dict]:
        qid = self.get_qid_by_label_and_description(label, description)
        if not qid:
            return None
        return self._load_item_by_qid(qid, language)

    def get_qid_by_label_and_claims(
        self,
        label: str,
        claim_filters: Dict[str, str],
        language: str = "en",
    ) -> Optional[str]:
        """
        Resolve a QID by label plus a set of literal property/value matches.
        claim_filters keys are property ids (e.g., 'P123'), values are literals.
        """
        if not claim_filters:
            return self.get_qid_by_label(label)

        qid = self.get_qid_by_label(label)
        if not qid:
            return None

        item = self._load_item_by_qid(qid, language)
        if not item:
            return None

        claims = item.get("claims", {})
        for pid, expected in claim_filters.items():
            values = self._extract_claim_values(
                {"claims": {pid: claims.get(pid, [])}},
                pid,
                None,
            )
            normalized_expected = self._normalize_unique_value(expected, None)
            if normalized_expected is None or normalized_expected not in values:
                return None
        return qid

    def _bulk_find_items_db(
        self,
        cursor: Any,
        labels: List[str],
    ) -> Dict[str, Optional[str]]:
        if not labels:
            return {}

        placeholders = ",".join(["%s"] * len(labels))
        query = f"""
        SELECT wbx_text as text, wbit_item_id as id 
        FROM wbt_item_terms 
        LEFT JOIN wbt_term_in_lang ON wbit_term_in_lang_id = wbtl_id 
        LEFT JOIN wbt_text_in_lang ON wbtl_text_in_lang_id = wbxl_id 
        LEFT JOIN wbt_text ON wbxl_text_id = wbx_id 
        WHERE wbtl_type_id = 1 AND wbx_text IN ({placeholders})
        """

        try:
            cursor.execute(query, labels)
            rows = cursor.fetchall()
        except Exception as exc:
            print(f"Error in bulk search: {exc}")
            return {}

        results: Dict[str, Optional[str]] = {}
        for text, item_id in rows:
            label_text = _decode_text(text).replace("\\'", "'")
            results[label_text] = f"Q{item_id}"
        return results

    def _select_qid_by_label(self, cursor: Any, label: str) -> Optional[str]:
        query = """
        SELECT wbit_item_id 
        FROM wbt_item_terms 
        LEFT JOIN wbt_term_in_lang ON wbit_term_in_lang_id = wbtl_id 
        LEFT JOIN wbt_text_in_lang ON wbtl_text_in_lang_id = wbxl_id 
        LEFT JOIN wbt_text ON wbxl_text_id = wbx_id 
        WHERE wbtl_type_id = 1 AND wbx_text = %s
        LIMIT 1
        """
        try:
            cursor.execute(query, [label])
            row = cursor.fetchone()
            if row:
                return f"Q{row[0]}"
        except Exception as exc:
            print(f"Error selecting qid by label: {exc}")
        return None

    def _select_qid_by_label_and_description(
        self,
        cursor: Any,
        label: str,
        description: str,
    ) -> Optional[str]:
        query = """
        SELECT label_terms.wbit_item_id
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
          AND labels.wbx_text = %s
          AND descriptions.wbx_text = %s
        LIMIT 1
        """
        try:
            cursor.execute(query, [label, description])
            row = cursor.fetchone()
            if row:
                return f"Q{row[0]}"
        except Exception as exc:
            print(f"Error selecting qid by label/description: {exc}")
        return None

    def find_items_by_labels_optimized(
        self,
        labels: List[str],
    ) -> Dict[str, Optional[str]]:
        if not labels:
            return {}

        normalized = [
            self._normalize_label(label) for label in labels if self._normalize_label(label)
        ]
        if not normalized:
            return {}

        normalized = list(dict.fromkeys(normalized))
        sanitized = [self._escape_label(lbl) for lbl in normalized]
        with self._db_cursor() as cursor:
            return self._bulk_find_items_db(cursor, sanitized)

    def _fetch_items_with_data(
        self,
        cursor: Any,
        labels: List[str],
        language: str = "en",
    ) -> List[Tuple[str, Optional[str], Any]]:
        if not labels:
            return []

        normalized = [self._normalize_label(lbl) for lbl in labels if self._normalize_label(lbl)]
        if not normalized:
            return []

        normalized = list(dict.fromkeys(normalized))
        rows: List[Tuple[str, Optional[str], Any]] = []

        sanitized = [self._escape_label(lbl) for lbl in normalized]
        placeholders = ",".join(["%s"] * len(sanitized))
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

        try:
            cursor.execute(query, sanitized)
            results = cursor.fetchall()
        except Exception as exc:
            print(f"Error fetching item data: {exc}")
            return rows

        for label_text, item_qid, item_json_text in results:
            decoded_label = _decode_text(label_text).replace("\\'", "'")
            rows.append((decoded_label, item_qid, item_json_text))

        return rows

    def _bulk_find_items_with_data_by_qid_db(
        self,
        cursor: Any,
        qids: List[str],
        language: str,
    ) -> Dict[str, dict]:
        if not qids:
            return {}

        items_by_qid: Dict[str, dict] = {}
        placeholders = ",".join(["%s"] * len(qids))
        query = f"""
        SELECT 
            page.page_title as qid,
            text.old_text as item_json
        FROM page
        LEFT JOIN text
            ON text.old_id = page.page_latest
        WHERE page.page_title IN ({placeholders})
        """

        try:
            cursor.execute(query, qids)
            results = cursor.fetchall()
        except Exception as exc:
            print(f"Error in QID data bulk search: {exc}")
            return items_by_qid

        for qid_text, item_json_text in results:
            qid = _decode_text(qid_text)
            if item_json_text:
                try:
                    if isinstance(item_json_text, bytes):
                        item_json_text = item_json_text.decode("utf-8")
                    item_json = json.loads(item_json_text)
                    claims = item_json.get("claims", {})
                    labels_dict = item_json.get("labels", {})
                    descriptions_dict = item_json.get("descriptions", {})

                    item_entity = entity(
                        labels=labels_dict if labels_dict else {},
                        aliases={},
                        descriptions=descriptions_dict if descriptions_dict else {},
                        claims=claims if claims else {},
                        etype="item",
                    )
                    item_entity["id"] = qid
                    items_by_qid[qid] = item_entity
                except (json.JSONDecodeError, Exception) as exc:
                    print(f"Warning: Could not parse item JSON for {qid}: {exc}")
                    items_by_qid[qid] = self._create_empty_item(qid, qid, language)
            else:
                items_by_qid[qid] = self._create_empty_item(qid, qid, language)

        return items_by_qid

    def _load_item_by_qid(self, qid: str, language: str = "en") -> Optional[dict]:
        with self._db_cursor() as cursor:
            results = self._bulk_find_items_with_data_by_qid_db(
                cursor, [qid], language=language
            )
        return results.get(qid)

    def _find_qids_by_label_and_description(
        self, pairs: List[Tuple[str, str]]
    ) -> Dict[Tuple[str, str], Optional[str]]:
        if not pairs:
            return {}

        sanitized: List[Tuple[str, str]] = []
        for label_value, description_value in pairs:
            label_norm = self._normalize_label(label_value)
            desc_norm = self._normalize_label(description_value)
            if label_norm and desc_norm:
                sanitized.append((self._escape_label(label_norm), self._escape_label(desc_norm)))

        if not sanitized:
            return {}

        placeholders = ",".join(["(%s, %s)"] * len(sanitized))
        params: List[str] = []
        for label_text, description_text in sanitized:
            params.extend([label_text, description_text])

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

        results: Dict[Tuple[str, str], Optional[str]] = {}
        with self._db_cursor() as cursor:
            try:
                cursor.execute(query, params)
                rows = cursor.fetchall()
            except Exception as exc:
                print(f"Error in label/description bulk search: {exc}")
                return results

        for label_text, description_text, item_id in rows:
            label_decoded = _decode_text(label_text).replace("\\'", "'")
            desc_decoded = _decode_text(description_text).replace("\\'", "'")
            results[(label_decoded, desc_decoded)] = f"Q{item_id}"

        return results

    def find_items_by_qids(
        self,
        qids: List[Any],
        language: str = "en",
    ) -> Dict[str, dict]:
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
        with self._db_cursor() as cursor:
            items_by_qid.update(
                self._bulk_find_items_with_data_by_qid_db(
                    cursor, normalized, language=language
                )
            )

        for qid in normalized:
            if qid not in items_by_qid:
                items_by_qid[qid] = self._create_empty_item(qid, qid, language)

        return items_by_qid

    def find_qids_by_unique_key(
        self,
        keys: List[Tuple[str, Optional[str]]],
        property_id: str,
        property_datatype: Optional[str] = None,
        language: str = "en",
    ) -> Dict[Tuple[str, Optional[str]], Optional[str]]:
        if not keys:
            return {}

        normalized_keys: List[Tuple[str, str]] = []
        for label, value in keys:
            norm_label = self._normalize_label(label)
            norm_value = self._normalize_unique_value(value, property_datatype)
            if norm_label and norm_value:
                normalized_keys.append((norm_label, norm_value))

        if not normalized_keys:
            return {}

        label_set = list(dict.fromkeys(label for label, _ in normalized_keys))
        with self._db_cursor() as cursor:
            rows = self._fetch_items_with_data(
                cursor, label_set, language=language
            )

        results: Dict[Tuple[str, Optional[str]], Optional[str]] = {}
        lookup: Dict[str, List[str]] = {}
        for label, value in normalized_keys:
            lookup.setdefault(label, []).append(value)

        for label_text, item_qid, item_json_text in rows:
            if not item_qid or not item_json_text:
                continue

            try:
                if isinstance(item_json_text, bytes):
                    item_json_text = item_json_text.decode("utf-8")
                item_json = json.loads(item_json_text)
            except Exception:
                continue

            claim_values = self._extract_claim_values(
                item_json, property_id, property_datatype
            )

            expected_values = lookup.get(label_text, [])
            for expected in expected_values:
                if expected in claim_values and (label_text, expected) not in results:
                    results[(label_text, expected)] = item_qid

        return results
