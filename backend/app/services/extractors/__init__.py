"""数据提取器模块

提供从搜索结果页和产品详情页提取数据的提取器类
"""
from .link_extractor import LinkExtractor
from .base_info_extractor import BaseInfoExtractor
from .dynamic_data_extractor import DynamicDataExtractor

__all__ = [
    "LinkExtractor",
    "BaseInfoExtractor",
    "DynamicDataExtractor",
]

