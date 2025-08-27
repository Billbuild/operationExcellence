
import pandas as pd
import numpy as np
from typing import Dict, Union
import logging


class DataCleaner:
    def __init__(self, config: Dict[str, Union[str, float]] = None):
        """
        智能数据清洗器
        :param config: 清洗配置 {
            'auto_correct_date': True,  # 自动校正日期格式
            'outlier_method': 'zscore',  # 异常值检测方法 (zscore/iqr)
            'numeric_threshold': 3.0,   # 数值型字段异常阈值
            'text_max_length': 100      # 文本字段最大长度
        }
        """
        self.config = config or {}
        self.logger = logging.getLogger(__name__)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """执行数据清洗流水线"""
        self._log_shape("原始数据", df)

        df = self.fill_missing(df)
        df = self.correct_dtypes(df)
        df = self.handle_outliers(df)
        df = self.clean_text(df)

        self._log_shape("清洗后数据", df)
        return df

    def fill_missing(self, df: pd.DataFrame) -> pd.DataFrame:
        # ... 现有缺失值处理代码 ...
        return df

    def correct_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        # ... 现有类型校正代码 ...
        return df

    def handle_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """智能异常值处理（新增核心方法）"""
        numeric_cols = df.select_dtypes(include=np.number).columns

        for col in numeric_cols:
            method = self.config.get('outlier_method', 'zscore')
            threshold = self.config.get('numeric_threshold', 3.0)

            if method == 'zscore':
                z_scores = (df[col] - df[col].mean()) / df[col].std()
                outliers = np.abs(z_scores) > threshold
            else:  # IQR 方法
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                outliers = (df[col] < (q1 - threshold * iqr)) | (df[col] > (q3 + threshold * iqr))

            # 用中位数替换异常值
            df.loc[outliers, col] = df[col].median()
            self.logger.info(f"处理 {col} 列的 {outliers.sum()} 个异常值")

        return df

    def clean_text(self, df: pd.DataFrame) -> pd.DataFrame:
        # ... 现有文本清洗代码 ...
        return df

    def _log_shape(self, prefix: str, df: pd.DataFrame):
        self.logger.debug(f"{prefix} - 行数: {len(df)}, 列数: {len(df.columns)}")


