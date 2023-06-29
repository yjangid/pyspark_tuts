from dataclasses import dataclass
from typing import Optional
import os

@dataclass
class Pipeline:
    key: str
    relative_path: str
    partitioned_by: Optional[str] = None
    output_format: Optional[str] = None

    @property
    def relative_path(self):
        return self._relative_path
    
    @relative_path.setter
    def relative_path(self, value):
        if value is not None:
            script_directory = os.path.dirname(os.path.abspath(__file__))
            project_directory = os.path.dirname(script_directory)
            file_path = os.path.join(project_directory, f"{value}")
        else:
            raise ValueError("file_path cann't be None")
        self._relative_path = file_path


def main():
    module_name = os.path.basename(__file__)
    print(f"Module - {module_name}")

if __name__ == "__main__":
    main()

