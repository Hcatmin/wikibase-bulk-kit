# Wikibase Bulk Kit

Framework modular para carga masiva de datasets CSV hacia Wikibase. Permite sincronizar esquemas (propiedades e ítems) y mapear datos desde archivos CSV hacia una instancia de Wikibase.

## Requisitos

- Python 3.12+
- Poetry
- Acceso a una instancia de Wikibase

## Instalación en Linux

### 1. Clonar el repositorio

```bash
git clone <url-del-repositorio>
cd wikibase-bulk-kit
```

### 2. Instalar Poetry (si no está instalado)

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

### 3. Instalar dependencias

```bash
poetry install
```

### 4. Configurar variables de entorno

Crear un archivo `.env` en la raíz del proyecto con las siguientes variables:

```env
# Configuración de Wikibase (requeridas)
wikibase_url=http://localhost:8181
mediawiki_api_url=http://localhost:8181/w/api.php
sparql_endpoint_url=http://localhost:8282/proxy/wdqs/bigdata/namespace/wdq/sparql

# Credenciales de Wikibase (requeridas)
wikibase_username=Admin
wikibase_password=your_password

# Configuración de MySQL (opcional, para indexación)
mysql_host=localhost
mysql_port=3306
mysql_database=my_wiki
mysql_user=wikiuser
mysql_password=sqlpass
```

## Comandos disponibles

Verificar que la instalación fue exitosa:

```bash
poetry shell
wbk --help
```

### `schema` - Sincronizar esquema

Sincroniza propiedades e ítems definidos en un archivo YAML hacia Wikibase.

```bash
wbk schema -p <ruta-al-schema.yml>
```

**Ejemplo:**

```bash
wbk schema -p configs/schema.yml
```

### `mapping` - Procesar mapeos CSV

Procesa archivos CSV y crea/actualiza ítems en Wikibase según la configuración de mapeo.

```bash
wbk mapping -p <ruta-al-mapping.yml>
```

**Ejemplo:**

```bash
wbk mapping -p configs/datasets.yml
```

### `indexing` - Construir tablas de indexación

Construye las tablas de indexación de Wikibase para optimizar búsquedas.

```bash
wbk indexing
```

### `links` - Actualizar tablas de enlaces

Actualiza las tablas de enlaces en Wikibase.

```bash
wbk links
```

## Configuraciones de ejemplo

### Esquema (`configs/schema.yml`)

Define las propiedades e ítems base que se sincronizarán en Wikibase:

```yaml
namespace: "Chile Educational System"
language: "es"

properties:
  - 
    label: "región"
    description: "Indica la región a la que pertenece un ítem"
    datatype: "wikibase-item"
    aliases: ["región administrativa", "región del país"]

  - 
    label: "comuna"
    description: "Especifica la comuna asociada a un ítem"
    datatype: "wikibase-item"
    aliases: ["municipio", "localidad"]

items:
  - 
    label: "comuna"
    description: "División administrativa local en Chile"
    aliases: ["municipio", "localidad"]
    statements:
      - 
        label: "instancia de"
        value: "clase"
        datatype: "wikibase-item"
```

**Tipos de datos soportados para propiedades:**
- `wikibase-item` - Referencia a otro ítem
- `quantity` - Valor numérico
- `time` - Fecha/hora
- `globe-coordinate` - Coordenadas geográficas
- `string` - Texto

### Mapeo de datos (`configs/datasets.yml`)

Define cómo se mapean los datos CSV hacia ítems en Wikibase:

```yaml
name: "Chilean Schools Mapping"
description: "Mapping configuration for Chilean educational establishments"
language: "es"

encoding: "utf-8"
delimiter: ";"
decimal_separator: ","

csv_files:
  - file_path: "data/directorio_2024_lite.csv"
    update_action: "replace_all"
    mappings:
      - item:
          label: "{NOM_RBD}"
          snak:
            property: "rol base de datos del establecimiento"
            value: "{RBD}"
        description: "Colegio {RBD} de la comuna de {NOM_COM_RBD}"
        statements:
          - property: "instancia de"
            value:
              label: "colegio"
          - property: "comuna"
            value: 
              label: "{NOM_COM_RBD}" # item debe existir previamente (mapear)
```

**Características del mapeo:**
- Uso de placeholders `{COLUMNA}` para referenciar valores del CSV
- Soporte para calificadores (qualifiers) en statements
- Estrategias de actualización: `replace_all`, `append_or_replace`
- Identificación de ítems mediante `snak` (propiedad + valor único)

## Estructura del proyecto

```
wikibase-bulk-kit/
├── wbk/                    # Código principal
│   ├── cli.py              # Punto de entrada CLI
│   ├── config/             # Configuración y settings
│   ├── mapping/            # Procesador de mapeos CSV
│   ├── schema/             # Sincronizador de esquemas
│   └── backend/            # Integración con Wikibase
├── configs/                # Archivos de configuración
│   ├── schema.yml          # Esquema de propiedades e ítems
│   └── datasets.yml        # Mapeo de datasets CSV
├── data/                   # Archivos CSV de datos
└── RaiseWikibase/          # Helpers legacy
```

## Flujo de trabajo típico

1. **Definir el esquema** en `configs/schema.yml` con las propiedades e ítems base
2. **Sincronizar el esquema** con `wbk schema -p configs/schema.yml`
3. **Configurar el mapeo** en un archivo YAML que referencie los CSV
4. **Ejecutar el mapeo** con `wbk mapping -p configs/datasets.yml`
5. **Construir índices** con `wbk indexing` y `wbk links`

