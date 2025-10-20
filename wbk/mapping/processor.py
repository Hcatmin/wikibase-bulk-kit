"""Mapping processor for transforming CSV data into Wikibase statements."""

import gc
import yaml
from pathlib import Path


from ..config.manager import ConfigManager
import pandas as pd
from RaiseWikibase.datamodel import entity, label, description, claim, snak
from RaiseWikibase.raiser import batch


from .models import MappingConfig, CSVFileConfig, ItemMapping, StatementMapping

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
        # cache of properties/items for syncing execution time
        self.cache: dict[str, str] = {}
        self.current_dataframe: pd.DataFrame | None = None
    
    
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

        # search for statements QIDs
        values_bulk_searcher = ItemBulkSearcher()
        values = values_bulk_searcher.find_items_by_labels_optimized(list_of_label_values)

        # search for items QIDs
        item_bulk_searcher = ItemBulkSearcher()
        items_found = item_bulk_searcher.find_items_by_labels_optimized(filtered_dataframe[item_mapping.label_column].tolist())

        # filter items not found
        filtered_dataframe = filtered_dataframe[~filtered_dataframe[item_mapping.label_column].isin(items_found.keys())]

        # create items
        self.bulk_create_items(filtered_dataframe, item_mapping, values)

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


    def bulk_create_items(self, df: pd.DataFrame, item_mapping: ItemMapping, values: dict[str, str] | None, chunk_size: int = 1000) -> None:
        """Create items in chunks to avoid memory issues
        
        Args:
            df: pandas DataFrame containing the data
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

            if values:
                for statement in item_mapping.statements:
                    if statement.value_column:
                        item['claims'].update(claim(prop=statement.property_id,
                                                    mainsnak=snak(datatype=statement.datatype,
                                                                value=values[row[statement.value_column]],
                                                                prop=statement.property_id,
                                                                snaktype='value')))
                    elif statement.value_label:
                        item['claims'].update(claim(prop=statement.property_id,
                                                    mainsnak=snak(datatype=statement.datatype,
                                                                value=values[statement.value_label],
                                                                prop=statement.property_id,
                                                                snaktype='value')))

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

    