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
```

## Enjoy
```bash
python3 main.py \
--database=production \
--table=graphite \
--key=Hostname \
--prefix=desktop01,desktop02 \
--await-mutation-end \
--config ./ch_cleaner.yaml
```


## Help message
```
usage: main.py [-h] [--prefix str [, ...]] [--key str]
                    [--database str] --table str [--checkout-only]
                    [--await-mutation-end] [--config file]

ClickHouse graphite metrics cleaner

optional arguments:
  -h, --help                            show this help message and exit

Commands:
  --prefix str [, ...], -p str [, ...]  Path prefix for search matches
  --key str, -k str                     Primary key (column) for prefix
                                        mathces
  --database str, -d str                Database to connect
  --table str, -t str                   Table for search matches
  --checkout-only                       Print only mutation status for table
  --await-mutation-end                  Lock script execution until the
                                        mutation completes
  --config file                         Custom path to config file in yaml
                                        format
```