import shutil
from pathlib import Path

from airflow.models.baseoperator import BaseOperator


class RemoveDirectoryOperator(BaseOperator):
    def __init__(self, directory: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.directory = directory

    def execute(self, context):
        try:
            shutil.rmtree(self.directory)
        except:
            pass

class CreateDirectoryOperatorPathlib(BaseOperator):
    def __init__(self, directory: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.directory = directory

    def execute(self, context):
        Path(self.directory).mkdir(parents=True, exist_ok=True)
