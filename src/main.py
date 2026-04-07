from database.init_db import init_db
from database.op import (
    insert_price_record,
    insert_price_records,
    get_price_record_by_ruid,
    list_price_records,
    query_price_records,
    delete_price_record_by_ruid,
    delete_price_records_by_gid,
    count_price_records,
)
init_db()
print("数据库已就绪")

from extract_data import data2list
from get_data import get_gids
from get_json import gid2json
import config

def main():
    config.get_env_cache()
    gids = set()
    gids = get_gids(20)

    for gid in gids:
        gid2json(gid)
        list = data2list(gid)
        insert_price_records(list)
        

        
        
        
    
    
if __name__ == "__main__":
    main()