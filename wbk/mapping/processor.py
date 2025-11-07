"""Mapping processor for transforming CSV data into Wikibase statements."""

import gc
import yaml
from pathlib import Path


from ..config.manager import ConfigManager
import pandas as pd
from RaiseWikibase.datamodel import entity, label, description, claim, snak
from RaiseWikibase.raiser import batch


from .models import MappingConfig, CSVFileConfig, ItemMapping, StatementMapping, UpdateAction

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

    def filter_dataframe(self, dataframe: pd.DataFrame, item_mapping: ItemMapping) -> pd.DataFrame:
        """Filter the dataframe to only include the columns needed for the item mapping"""
        statements = item_mapping.statements
        statements_with_column = filter(lambda x: x.value_column is not None, statements)

        columns = [statement.value_column for statement in statements_with_column]
        dataframe = dataframe[[item_mapping.label_column] + columns]
        
        dataframe = dataframe.dropna(subset=[item_mapping.label_column])

        filtered_dataframe = dataframe.drop_duplicates()
        del dataframe
        gc.collect()
        return filtered_dataframe

    def process_item_mapping(self, item_mapping: ItemMapping) -> None:
        # filter only needed columns
        filtered_dataframe = self.filter_dataframe(self.current_dataframe, item_mapping)
        list_of_label_values = self.get_label_by_column(filtered_dataframe, item_mapping.statements)

        self.update_pids_by_labels(item_mapping.statements)
        item_bulk_searcher = ItemBulkSearcher()
        items_found = item_bulk_searcher.find_items_by_labels_optimized(list_of_label_values)
        self.qids_by_labels.update(items_found)


        # search for items QIDs
        item_bulk_searcher = ItemBulkSearcher()
        items_found = item_bulk_searcher.find_items_by_labels_optimized(
            filtered_dataframe[item_mapping.label_column].tolist()
        )

        # update items
        if item_mapping.update_action:
            df_items_found = filtered_dataframe[filtered_dataframe[item_mapping.label_column].isin(items_found.keys())]
            self.bulk_update_items(df_items_found, item_mapping)

        # create items
        df_items_not_found = filtered_dataframe[~filtered_dataframe[item_mapping.label_column].isin(items_found.keys())]
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

    def get_label_by_column(self, dataframe: pd.DataFrame, statements: list[StatementMapping] | None) -> list[str]:
        # retrieve all labels from statement.value_column

        if statements is None:
            return []

        # filter statement that doesnt have value_column 
        statements_with_column = filter(lambda x: x.value_column is not None, statements)

        columns = [statement.value_column for statement in statements_with_column]

        list_of_labels = []
        for col in columns:
            column = dataframe[col]
            column = column.drop_duplicates()
            column = column.dropna()
            column = column.tolist()
            list_of_labels += column

        statement_with_label = filter(lambda x: x.value_label is not None, statements)
        list_of_labels += [statement.value_label for statement in statement_with_label]

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

    def add_claims(self, item: entity, row: pd.Series, statement: StatementMapping) -> None:
        if statement.property_id:
            property_id = statement.property_id
        elif statement.property_label:
            property_id = self.pids_by_labels[statement.property_label]
        else:
            raise ValueError(f"No property id or property label found for statement: {statement}")

        if statement.datatype == 'wikibase-item':
            if statement.value_column:
                value = self.qids_by_labels[row[statement.value_column]]
            elif statement.value_label:
                value = self.qids_by_labels[statement.value_label]
            elif statement.value:
                value = statement.value
            else:
                raise ValueError(f"No value column or value label found for statement: {statement}")
        else:
            if statement.value_column:
                value = row[statement.value_column]
            elif statement.value:
                value = statement.value
            else:
                raise ValueError(f"No value column or value label found for statement: {statement}")

        item['claims'].update(claim(prop=property_id,
                                    mainsnak=snak(datatype=statement.datatype,
                                                value=value,
                                                prop=property_id,
                                                snaktype='value')))

        

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

