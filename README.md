**毕业设计**

在Python 3.10.16环境下开发

基于大语言模型的汽车价格监测系统

**关键词：** 大语言模型；数据提取；非结构化数据

This project extracts data of prices of cars from **forum** discussing texts. It first does data cleaning locally (btw this step also can be replaced with LLM. And I have coded this function but never tested and used.), then uses LLM to extract price records from the preprecessed data.

This project uses **SQLite** to store all the fetched data.
