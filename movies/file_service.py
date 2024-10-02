import os
import shutil
import uuid


class FileService:

    def file_exists(self, file_path):
        return os.path.exists(file_path)

    def create_dirs(self, dir_name):
        os.makedirs(dir_name, exist_ok=True)

    def get_files(self, path):
        files = []

        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                files.append(file_path)

        if not files:
            print("No new source file to process")

        files = sorted(files)
        for file_path in reversed(files):
            yield file_path

    def move_file(self, file_path, destination_path):
        try:
            if not os.path.exists(destination_path):
                os.makedirs(destination_path)
            file_name = os.path.basename(file_path)
            file_name = f"{uuid.uuid4()}_{file_name}"
            destination_path = os.path.join(destination_path, file_name)
            shutil.move(file_path, destination_path)

            print(f"Moved {file_path} to {destination_path}")
        except Exception as e:
            print(f"Error: File not moved - {file_path}: {e}")

    def delete_file(self, file_path):
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"Deleted file: {file_path}")
            else:
                print(f"File not found: {file_path}")
        except Exception as e:
            print(f"Error: File not deleted - {file_path}: {e}")
