Clickhouse metrics cleaner
==========================

Deletes data that matches (match_key, prefix).


## Prepare you config file
* Rename file and move to `~/.config/`:  
    ```bash
    mv ch_cleaner.yaml.example ~/.config/ch_cleaner.yaml
    vim ~/.config/ch_cleaner.yaml
    ```  
* Also, you can use `--config path/to/file.yaml` arg.
* Example:
```yaml
clickhouse:
  fqdn: my-clickhouse.example.com
  http_port: 8123
  database: production  # optional, can be passed as an argument
  match_key: Hostname  # optional, can be passed as an argument
  user: default
  password: my-strong-password
```

## Create virtual enviroment and install requirements
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
./clickhouse_cleaner.py
```

## Also, you can setup the script
```bash
cd clickhouse-cleaner
make install
make clean
clickhouse-cleaner --help
```

## Enjoy
```bash
clickhouse-cleaner \
--database=production \
--table=graphite,graphite_index \
--key=Hostname \
--prefix=desktop01,desktop02 \
--await-mutation-end \
--config ./ch_cleaner.yaml
```


## Help message
```
usage: clickhouse-cleaner [-h] [--prefix str [, ...]] [--key str] [--database str]
                          --table str [, ...] [--checkout-only] [--await-mutation-end]
                          [--force] [--config file]

ClickHouse data cleaner

optional arguments:
  -h, --help                            show this help message and exit

Arguments:
  --prefix str [, ...], -p str [, ...]  Prefixes for searching for matches
  --key str, -k str                     Primary key in the table for searching
                                        for matches by prefix
  --database str, -d str                Database to connect
  --table str [, ...], -t str [, ...]   Tables for search
  --checkout-only, -S                   Print only mutation status for table
  --await-mutation-end, -W              Lock script execution until the
                                        mutation completes
  --force, -f                           Delete all matches without asking for
                                        confirmation (pretty output)
  --config file, -c file                Custom path to config file in yaml
                                        format
```
