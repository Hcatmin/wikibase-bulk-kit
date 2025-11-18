from wbk.cli import mapping
from wbk.config.manager import ConfigManager
from wbk.mapping.processor import MappingProcessor

from wbk.processor.bulk_item_search import ItemBulkSearcher

def main():
    config_manager = ConfigManager('configs/project.yml')

    mapping_processor = MappingProcessor(config_manager)
    mapping_processor.process('configs/datosabiertos.mineduc.cl/directorio_ee/mapping.yml')


    # schema sync

    # item_searcher = ItemBulkSearcher()

    # qids = item_searcher.find_qids([('INSTITUTO CHACABUCO', 'Colegio de la comuna de LOS ANDES')])
    # items = item_searcher.find_items([('INSTITUTO CHACABUCO', 'Colegio de la comuna de LOS ANDES')])
    # items = item_searcher.find_items_by_labels_with_data(['ESCUELA BASICA'])

    # print(qids)
    # print(items)


if __name__ == "__main__":
    main()