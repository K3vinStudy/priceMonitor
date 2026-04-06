# 作者：郑凯峰

from database.init_db import init_db
from database.op import (
    create_project,
    get_project_by_id,
    get_all_projects,
    update_project,
    delete_project,
)
init_db()
print("数据库已就绪")

from get_data import get_gids
from get_json import gid2json
# from LLM import url2json
import config
import extract_data

def main():
    config.get_env_cache()
    gids = set()
    gids = get_gids()
    # results = [save_html_with_gid(x) for x in gids]
    # for x in gids:
    #     save_html_with_gid(x)
    # print(results)
    for gid in gids:
        # json = url2json("https://www.dongchedi.com/ugc/article/" + gid)
        gid2json(gid)
        # print()
        # print("###### GID: " + gid + " ######")
        # print()
        # print(json)
        
        
        
    
    
if __name__ == "__main__":
    main()