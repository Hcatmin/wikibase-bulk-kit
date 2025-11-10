"""Mapping processor for transforming CSV data into Wikibase statements."""

import gc
import yaml
from pathlib import Path


from ..config.manager import ConfigManager
import pandas as pd
from RaiseWikibase.datamodel import entity, label, description, claim, snak
from RaiseWikibase.raiser import batch


from .models import MappingConfig, CSVFileConfig, ItemMapping, StatementMapping, UpdateAction, ClaimMapping

from RaiseWikibase.dbconnection import DBConnection

from wbk.processor.bulk_item_search import ItemBulkSearcher


class MappingProcessor:
    """Processes CSV files and applies column mappings to create Wikibase statements."""
    
    def __init__(self, config_manager: ConfigManager) -> None:
        """Initialize schema syncer.
        
        Args:
            config_manager: Configuration manager instance
        """
        self.config_manager = config_manager
        self.language: str = 'en'
        self.wbi = config_manager.get_wikibase_integrator()
        self.current_dataframe: pd.DataFrame | None = None

        self.pids_by_labels: dict[str, str] = {}
        self.qids_by_labels: dict[str, str] = {}
    
    def _load_mapping_config(self, mapping_path: str) -> MappingConfig:
        mapping_file = Path(mapping_path)
        if not mapping_file.exists():
            raise FileNotFoundError(f"Mapping file not found: {mapping_path}")
        
        with open(mapping_file, 'r', encoding='utf-8') as f:
            mapping_data = yaml.safe_load(f)
        
        return MappingConfig(**mapping_data)

    def process(self, mapping_path: str) -> None:
        """Process the mapping configuration.
        
        Args:
            mapping_path: Path to the mapping configuration file
        """
        mapping_config = self._load_mapping_config(mapping_path)
        self.language = mapping_config.language
        
        for csv_file_config in mapping_config.csv_files:
            self.current_dataframe = pd.read_csv(
                csv_file_config.file_path, 
                encoding=csv_file_config.encoding,
                delimiter=csv_file_config.delimiter,
                decimal=csv_file_config.decimal_separator
            )
            self.process_item_mappings(csv_file_config)

    def process_item_mappings(self, csv_file_config: CSVFileConfig) -> None:
        """Process the item mappings.
        
        Args:
            csv_file_config: Configuration for the CSV file
        """

        for item_mapping in csv_file_config.item_mapping:
            print(f"Processing item mapping: {item_mapping.label_column}")
            self.process_item_mapping(item_mapping)

    def _extract_columns_from_value_spec(
        self, 
        value_spec: str | dict | list | None
    ) -> list[str]:
        """Extract column names from a value specification.
        
        Args:
            value_spec: Value specification (str, dict, or list)
            
        Returns:
            List of column names referenced in the value spec
        """
        columns = []
        
        if value_spec is None:
            return columns
        
        if isinstance(value_spec, str):
            # Shorthand: treat as column name
            columns.append(value_spec)
        elif isinstance(value_spec, dict):
            if 'column' in value_spec:
                columns.append(value_spec['column'])
        elif isinstance(value_spec, list):
            # Recursively extract columns from each element
            for elem in value_spec:
                columns.extend(self._extract_columns_from_value_spec(elem))
        
        return columns

    def _extract_labels_from_value_spec(
        self,
        value_spec: str | dict | list | None,
        datatype: str,
        dataframe: pd.DataFrame | None = None
    ) -> list[str]:
        """Extract labels/values from a value specification for wikibase-item lookup.
        
        Args:
            value_spec: Value specification (str, dict, or list)
            datatype: The datatype of the property
            dataframe: Optional dataframe to extract column values from
            
        Returns:
            List of labels/values (only for wikibase-item datatype)
        """
        labels = []
        
        if value_spec is None:
            return labels
        
        # Only extract labels for wikibase-item datatype
        if datatype != 'wikibase-item':
            return labels
        
        if isinstance(value_spec, str):
            # Shorthand: treat as column name
            if dataframe is not None and value_spec in dataframe.columns:
                labels.extend(
                    dataframe[value_spec].drop_duplicates().dropna().tolist()
                )
        elif isinstance(value_spec, dict):
            if 'column' in value_spec:
                if dataframe is not None and value_spec['column'] in dataframe.columns:
                    labels.extend(
                        dataframe[value_spec['column']]
                        .drop_duplicates()
                        .dropna()
                        .tolist()
                    )
            elif 'label' in value_spec:
                labels.append(value_spec['label'])
            elif 'value' in value_spec:
                # Static value - if it's not a QID, treat as label
                val = value_spec['value']
                if not (val.startswith('Q') and val[1:].isdigit()):
                    labels.append(val)
        elif isinstance(value_spec, list):
            # For tuples, extract labels from each element
            for elem in value_spec:
                labels.extend(
                    self._extract_labels_from_value_spec(elem, datatype, dataframe)
                )
        
        return labels

    def filter_dataframe(
        self, 
        dataframe: pd.DataFrame, 
        item_mapping: ItemMapping
    ) -> pd.DataFrame:
        """Filter the dataframe to only include the columns needed for the item mapping"""
        columns = []
        
        if item_mapping.statements:
            for statement in item_mapping.statements:
                # Extract columns from statement value
                columns.extend(
                    self._extract_columns_from_value_spec(statement.value)
                )
                
                # Extract columns from qualifiers
                if statement.qualifiers:
                    for qual in statement.qualifiers:
                        columns.extend(
                            self._extract_columns_from_value_spec(qual.value)
                        )
                
                # Extract columns from references
                if statement.references:
                    for ref in statement.references:
                        columns.extend(
                            self._extract_columns_from_value_spec(ref.value)
                        )
        
        # Remove duplicates and ensure label_column is included
        all_columns = list(set([item_mapping.label_column] + columns))
        
        dataframe = dataframe[all_columns]
        dataframe = dataframe.dropna(subset=[item_mapping.label_column])
        filtered_dataframe = dataframe.drop_duplicates()
        del dataframe
        gc.collect()
        return filtered_dataframe

    def process_item_mapping(self, item_mapping: ItemMapping) -> None:
        # filter only needed columns
        filtered_dataframe = self.filter_dataframe(self.current_dataframe, item_mapping)
        list_of_label_values = self.get_label_by_column(filtered_dataframe, item_mapping)

        self.update_pids_by_labels(item_mapping.statements)
        self.update_qids_by_labels(list_of_label_values)

        # update items
        if item_mapping.update_action:
            df_items_found = filtered_dataframe[
                filtered_dataframe[item_mapping.label_column].isin(self.qids_by_labels.keys())
            ]
            self.bulk_update_items(df_items_found, item_mapping)
        else: # create items
            df_items_not_found = filtered_dataframe[
                ~filtered_dataframe[item_mapping.label_column].isin(self.qids_by_labels.keys())
            ]
            self.bulk_create_items(df_items_not_found, item_mapping)

    def update_pids_by_labels(self, statements: list[StatementMapping] | None) -> None:
        """Update Cache of the PIDs by labels"""

        db_connection = DBConnection()

        statements_with_label = filter(lambda x: x.property_label is not None, statements)
        for statement in statements_with_label:
            property_label = statement.property_label
            self.pids_by_labels[property_label] = db_connection.find_property_id(property_label)
    
    def update_qids_by_labels(self, labels: list[str]) -> None:
        """Update Cache of the QIDs by labels"""
        item_bulk_searcher = ItemBulkSearcher()
        items_found = item_bulk_searcher.find_items_by_labels_optimized(labels)
        self.qids_by_labels.update(items_found)

    def get_label_by_column(
        self, 
        dataframe: pd.DataFrame, 
        item_mapping: ItemMapping
    ) -> list[str]:
        """Retrieve all labels from value specifications for wikibase-item lookups."""
        label_column = item_mapping.label_column
        statements = item_mapping.statements
        
        if label_column is None and statements is None:
            return []

        list_of_labels = []

        # Get labels from label_column
        if label_column and label_column in dataframe.columns:
            list_of_labels += (
                dataframe[label_column].drop_duplicates().dropna().tolist()
            )

        # Extract labels from statements, qualifiers, and references
        if statements:
            for statement in statements:
                # Extract labels from statement value
                list_of_labels.extend(
                    self._extract_labels_from_value_spec(
                        statement.value, 
                        statement.datatype, 
                        dataframe
                    )
                )
                
                # Extract labels from qualifiers
                if statement.qualifiers:
                    for qual in statement.qualifiers:
                        list_of_labels.extend(
                            self._extract_labels_from_value_spec(
                                qual.value,
                                qual.datatype,
                                dataframe
                            )
                        )
                
                # Extract labels from references
                if statement.references:
                    for ref in statement.references:
                        list_of_labels.extend(
                            self._extract_labels_from_value_spec(
                                ref.value,
                                ref.datatype,
                                dataframe
                            )
                        )

        return list_of_labels

    def bulk_create_items(self, df: pd.DataFrame, item_mapping: ItemMapping, chunk_size: int = 1000) -> None:
        """Create items in chunks to avoid memory issues
        
        Args:
            df: pandas DataFrame containing the datareplace_all
            language: Language code for labels and descriptions
            name_column: Column name containing the item names
            description_column: Column name containing descriptions (optional)
            chunk_size: Number of items to process in each batch
        """
        items = []
        
        for i, (_, row) in enumerate(df.iterrows()):
            # Create item entity
            item_name = str(row[item_mapping.label_column])
            labels = label(self.language, item_name)
            
            # Handle description if column is provided
            if item_mapping.description and item_mapping.description in df.columns:
                item_description = str(row[item_mapping.description])
                descriptions = description(self.language, item_description)
            else:
                # Create empty descriptions dict instead of None
                descriptions = {}

            item = entity(
                labels=labels,
                aliases={},  # Empty aliases dict
                descriptions=descriptions,
                claims={},
                etype='item'
            )

            for statement in item_mapping.statements:
                self.add_claims(item, row, statement)

            items.append(item)
            
            # Process in chunks
            if len(items) >= chunk_size:
                try:
                    print(f"Creating batch of {len(items)} items...")
                    result = batch('wikibase-item', items)
                    print(f"✓ Successfully created batch of {len(items)} items (total: {i+1})")
                except Exception as e:
                    print(f"✗ Error creating batch of {len(items)} items: {e}")
                    import traceback
                    traceback.print_exc()
                    raise
                items = []
        
        # Process remaining items
        if items:
            print(f"Final batch item sample: {items[0]}")
            try:
                print(f"Creating final batch of {len(items)} items...")
                result = batch('wikibase-item', items)
                print(f"✓ Successfully created final batch of {len(items)} items")
            except Exception as e:
                print(f"✗ Error creating final batch of {len(items)} items: {e}")
                import traceback
                traceback.print_exc()
                raise
        
        self._verify_items_created()

    def _resolve_value(
        self,
        value_spec: str | dict | list | None,
        row: pd.Series,
        datatype: str
    ) -> str | tuple:
        """Resolve a value specification to an actual value or tuple.
        
        Args:
            value_spec: Value specification (str, dict, or list)
            row: The pandas Series row containing data
            datatype: The datatype of the property
            
        Returns:
            Resolved value (str for simple types, tuple for complex types)
        """
        if value_spec is None:
            raise ValueError(f"No value specified for datatype {datatype}")
        
        # Helper to resolve a single value spec element
        def resolve_element(elem):
            if isinstance(elem, str):
                # Shorthand: treat as column name
                return row[elem]
            elif isinstance(elem, dict):
                if 'column' in elem:
                    return row[elem['column']]
                elif 'value' in elem:
                    return elem['value']
                elif 'label' in elem:
                    # For wikibase-item, lookup QID by label
                    if datatype == 'wikibase-item':
                        return self.qids_by_labels[elem['label']]
                    else:
                        return elem['label']
                else:
                    raise ValueError(
                        f"Invalid value spec dict: {elem}. "
                        f"Must have 'column', 'value', or 'label' key"
                    )
            else:
                raise ValueError(
                    f"Invalid value spec element: {elem}. "
                    f"Must be str or dict"
                )
        
        # If it's a list, construct a tuple
        if isinstance(value_spec, list):
            resolved_elements = [resolve_element(elem) for elem in value_spec]
            return tuple(resolved_elements)
        
        # Single value - resolve it
        resolved = resolve_element(value_spec)
        
        # For wikibase-item datatype, if we got a string (label), 
        # look it up in qids_by_labels
        if datatype == 'wikibase-item' and isinstance(resolved, str):
            # Check if it's already a QID (starts with Q)
            if resolved.startswith('Q') and resolved[1:].isdigit():
                return resolved
            # Otherwise, treat as label and lookup
            return self.qids_by_labels.get(resolved, resolved)
        
        return resolved

    def _create_snak_from_claim_mapping(
        self, 
        claim_mapping: ClaimMapping, 
        row: pd.Series
    ) -> dict:
        """Create a snak from a ClaimMapping.
        
        Args:
            claim_mapping: The ClaimMapping to convert
            row: The pandas Series row containing data
            
        Returns:
            A snak dictionary
        """
        # Get property ID
        if claim_mapping.property_id:
            prop_id = claim_mapping.property_id
        elif claim_mapping.property_label:
            prop_id = self.pids_by_labels[claim_mapping.property_label]
        else:
            raise ValueError(
                f"No property id or property label found for "
                f"claim mapping: {claim_mapping}"
            )
        
        # Resolve value using unified value field
        value = self._resolve_value(
            claim_mapping.value,
            row,
            claim_mapping.datatype
        )
        
        return snak(
            datatype=claim_mapping.datatype,
            value=value,
            prop=prop_id,
            snaktype='value'
        )

    def add_claims(
        self, 
        item: entity, 
        row: pd.Series, 
        statement: StatementMapping
    ) -> None:
        if statement.property_id:
            property_id = statement.property_id
        elif statement.property_label:
            property_id = self.pids_by_labels[statement.property_label]
        else:
            raise ValueError(
                f"No property id or property label found for "
                f"statement: {statement}"
            )

        # Resolve value using unified value field
        value = self._resolve_value(
            statement.value,
            row,
            statement.datatype
        )

        if any(map(lambda x: x == ' ', value)):
            return
            
        # Process qualifiers
        qualifiers = []
        if statement.qualifiers:
            for qualifier_mapping in statement.qualifiers:
                qualifier_snak = self._create_snak_from_claim_mapping(
                    qualifier_mapping, row
                )
                qualifiers.append(qualifier_snak)
        
        # Process references
        references = []
        if statement.references:
            for reference_mapping in statement.references:
                reference_snak = self._create_snak_from_claim_mapping(
                    reference_mapping, row
                )
                references.append(reference_snak)

        # Create claim with qualifiers and references
        mainsnak_dict = snak(
            datatype=statement.datatype,
            value=value,
            prop=property_id,
            snaktype='value'
        )
        
        # Handle rank if provided
        rank = statement.rank if statement.rank else 'normal'
        
        claim_dict = claim(
            prop=property_id,
            mainsnak=mainsnak_dict,
            qualifiers=qualifiers,
            references=references
        )
        
        # Update rank if provided
        if statement.rank and statement.rank != 'normal':
            claim_dict[property_id][0]['rank'] = statement.rank
        
        item['claims'].update(claim_dict)

    def bulk_update_items(self, df: pd.DataFrame, item_mapping: ItemMapping) -> None:
        update_action = item_mapping.update_action

        if update_action == UpdateAction.REPLACE_ALL:
            self.bulk_replace_items(df, item_mapping)
        elif update_action == UpdateAction.APPEND_OR_REPLACE:
            self.bulk_append_or_replace_items(df, item_mapping)
        elif update_action == UpdateAction.FORCE_APPEND:
            self.bulk_force_append_items(df, item_mapping)
        elif update_action == UpdateAction.KEEP:
            self.bulk_keep_items(df, item_mapping)
        elif update_action == UpdateAction.MERGE_REFS_OR_APPEND:
            self.bulk_merge_refs_or_append_items(df, item_mapping)
        else:
            raise ValueError(f"Invalid update action: {update_action}")

    def bulk_replace_items(
        self, 
        items_to_update: pd.DataFrame, 
        item_mapping: ItemMapping,
        chunk_size: int = 1000
    ) -> None:
        if items_to_update.empty:
            return

        items = []

        item_bulk_searcher = ItemBulkSearcher()
        qids_to_update = item_bulk_searcher.find_items_by_labels_optimized(
            items_to_update[item_mapping.label_column].tolist()
        )
        
        for i, (_, row) in enumerate(items_to_update.iterrows()):
            # Get the QID for this item from cache
            item_label = str(row[item_mapping.label_column])
            item_qid = qids_to_update.get(item_label)
            
            # Skip if QID not found (shouldn't happen, but safety check)
            if not item_qid:
                continue
            
            # Create item entity with existing QID
            labels = label(self.language, item_label)
            
            # Handle description if column is provided
            if (item_mapping.description and 
                item_mapping.description in items_to_update.columns):
                item_description = str(row[item_mapping.description])
                descriptions = description(self.language, item_description)
            else:
                descriptions = {}
            
            item = entity(
                labels=labels,
                aliases={},
                descriptions=descriptions,
                claims={},
                etype='item'
            )
            
            # Set the existing QID
            item['id'] = item_qid
            
            # Replace all claims (REPLACE_ALL action)
            if item_mapping.statements:
                for statement in item_mapping.statements:
                    self.add_claims(item, row, statement)

            items.append(item)
            
            # Process in chunks
            if len(items) >= chunk_size:
                try:
                    print(
                        f"Updating batch of {len(items)} items "
                        f"(total: {i+1})..."
                    )
                    result = batch('wikibase-item', items, new=False)
                    print(
                        f"✓ Successfully updated batch of {len(items)} items "
                        f"(total: {i+1})"
                    )
                except Exception as e:
                    print(
                        f"✗ Error updating batch of {len(items)} items: {e}"
                    )
                    import traceback
                    traceback.print_exc()
                    raise
                items = []
        
        # Process remaining items
        if items:
            try:
                print(f"Updating final batch of {len(items)} items...")
                result = batch('wikibase-item', items, new=False)
                print(
                    f"✓ Successfully updated final batch of "
                    f"{len(items)} items"
                )
            except Exception as e:
                print(
                    f"✗ Error updating final batch of {len(items)} items: {e}"
                )
                import traceback
                traceback.print_exc()
                raise

    def bulk_append_or_replace_items(self):
        # TODO: Implement this
        pass

    def bulk_force_append_items(self):
        # TODO: Implement this
        pass

    def bulk_keep_items(self):
        # TODO: Implement this
        pass

    def bulk_merge_refs_or_append_items(self):
        # TODO: Implement this
        pass

    def _verify_items_created(self) -> None:
        """Verify that items were actually created in the database."""
        try:
            connection = DBConnection()
            
            # Get the latest item ID
            latest_eid = connection.get_last_eid(content_model='wikibase-item')
            print(f"Latest item ID in database: Q{latest_eid}")
            
            # Check if we can find some of the created items
            cursor = connection.conn.cursor()
            cursor.execute("""
                SELECT page_title, page_id 
                FROM page 
                WHERE page_namespace = 0 
                ORDER BY page_id DESC 
                LIMIT 10
            """)
            
            recent_items = cursor.fetchall()
            print(f"Recent items in database: {recent_items}")
            
        except Exception as e:
            print(f"Warning: Could not verify items in database: {e}")

