import luigi
from luigi.util import requires
import os
import wget
import tarfile
import gzip 
import io
import pandas as pd

    
DEFAULT_PARAMS = {
    'dataset_name': 'GSE68849',
    'folder_name': 'datasets',
    'cols_to_delete': [
        'Definition', 
        'Ontology_Component', 
        'Ontology_Process', 
        'Ontology_Function', 
        'Synonyms', 
        'Obsolete_Probe_Id', 
        'Probe_Sequence'
    ]
}

# Шаг 1. Создание папки и скачивание в нее датасета
class DownloadDatasetTask(luigi.Task):
    dataset_name = luigi.Parameter()
    folder_name = luigi.Parameter()

    def run(self):
        os.makedirs(self.dir_name, exist_ok=True) # создание основной папки для датасета
        url = f'https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset_name}&format=file'
        output_path = self.output().path
        wget.download(url, out=output_path)
        
    def output(self):
        self.dir_name = f"{self.folder_name}/{self.dataset_name}"
        return luigi.LocalTarget(f'{self.dir_name}/{self.dataset_name}_RAW.tar')
# Конец шага 1        


# Шаг 2. Распаковка датасета
@requires(DownloadDatasetTask)
class UnpackTarTask(luigi.Task):
    '''
    Извлекаем gzip-файлы из tar-архива в отдельные папки и сразу же разархивируем их.
    Input: путь к tar-архиву
    Output: список путей к txt-файлам, распакованным из gzip-архивов
    '''
    paths = []
       
    def run(self):
        self.dir, self.file = os.path.split(str(self.input()))
        os.chdir(self.dir)
        
        with tarfile.open(self.file, 'r') as tar:
            for member in tar.getmembers():
                if member.name.endswith('.gz'): # если это gzip-архив
                    # создаем папку с именем архива
                    folder_name = member.name.split('.')[0]
                    os.makedirs(folder_name, exist_ok=True)
                    # достаем gzip из tar и кладем его в созданную папку
                    tar.extract(member, path=folder_name, filter='data')
                    # получаем путь к gzip-архиву
                    gzip_path = os.path.abspath(f'{folder_name}/{member.name}')
                    # задаем новый путь для распаковки gzip-архива
                    out_path = gzip_path.replace('.gz', '')
                    # распаковываем gzip-архив
                    with gzip.open(gzip_path, 'rb') as f_in, open(out_path, 'wb') as f_out:
                        f_out.write(f_in.read())   
                    
                    self.paths.append(out_path)
                    os.remove(gzip_path)
    
    def output(self):
        return [luigi.LocalTarget(i) for i in self.paths] 
    
    
@requires(UnpackTarTask)
class SplitTablesTask(luigi.Task):
    '''
    Разбиваем txt-файлы на отдельные tsv-таблицы.
    Input: список путей к txt-файлам
    Output: список путей к tsv-файлам 'Probes', которые предстоит обработать в следующем таске
    '''
    paths = []
    
    def run(self):
        for path in self.input():
            path = str(path)
            dir = os.path.split(path)[0]
            
            # ----------------- Код для разделения таблиц из условия задания ---------------
            dfs = {}
            with open(path) as f:
                write_key = None
                fio = io.StringIO()
                for l in f.readlines():
                    if l.startswith('['):
                        if write_key:
                            fio.seek(0)
                            header = None if write_key == 'Heading' else 'infer'
                            dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                        fio = io.StringIO()
                        write_key = l.strip('[]\n')    
                        continue
                    if write_key:
                        fio.write(l)
                fio.seek(0)
                dfs[write_key] = pd.read_csv(fio, sep='\t')
                # -------------------------------- END --------------------------------
                
            for name, df in dfs.items():
                path = os.path.join(dir, name+'.tsv')
                df.to_csv(path, sep='\t', encoding='utf-8')
                if name == 'Probes':
                    self.paths.append(path)                    
                
    def output(self):
        return [luigi.LocalTarget(i) for i in self.paths]
# Конец шага 2

# Шаг 3. Создание урезанных вариантов таблиц Probes    
@requires(SplitTablesTask)
class CropProbesTask(luigi.Task):
    '''
    Создаем урезанные версии таблиц Probes, из которых удалены колонки из переданного в качестве параметра списка.
    Полные версии таблиц Probes не удаляются.
    Input: список путей к таблицам Probes
    Output: список путей к урезанным версиям таблиц Probes (проверяем, что они созданы)
    '''
    paths = []
    cols_to_delete = luigi.ListParameter()
    
    def run(self):
        for path in self.input():
            path = str(path)
            dir, file = os.path.split(path)
            filename, ext = file.split('.')
            out_file = '.'.join([filename + '_cropped', ext])
            out_path = os.path.join(dir, out_file)
            
            df = pd.read_csv(path, sep='\t')
            
            for col in self.cols_to_delete:
                if col in df.columns:
                    df = df.drop(col, axis=1)
            
            df.to_csv(out_path, sep='\t', encoding='utf-8')
            self.paths.append(out_path)
            
    def output(self):
        return [luigi.LocalTarget(i) for i in self.paths]
# Конец шага 3

# Шаг 4. Удаление изначальных текстовых файлов с таблицами
@requires(CropProbesTask)  
class RemoveTxtTask(luigi.Task):
    '''
    Input: список путей к урезанным версиям таблиц Probes, созданным в предыдущем таске
    Output: пустышка, позволяющая убедиться, что предыдущие таски выполнены. Без нее WrapperTask выдаст ошибку 'RuntimeError: Unfulfilled dependency at run time', поскольку у этого таска нет output-а в виде файлов.
    '''
    def run(self):
        for path in self.input():
            dir = os.path.dirname(str(path))
            
            for file in os.listdir(dir):
                if file.endswith('.txt'):
                    os.remove(os.path.join(dir, file))
                    
    def complete(self):
        return self.requires().complete() 
# Конец шага 4


@requires(RemoveTxtTask)
class Pipeline(luigi.WrapperTask):
    '''
    Таск-обертка для запуска всей цепочки. Здесь же задаются дефолтные значения параметров.
    ''' 
    dataset_name = luigi.Parameter(default=DEFAULT_PARAMS['dataset_name'])
    folder_name = luigi.Parameter(default=DEFAULT_PARAMS['folder_name'])
    cols_to_delete = luigi.Parameter(default=DEFAULT_PARAMS['cols_to_delete'])

    def complete(self):
        return all(r.complete() for r in luigi.task.flatten(self.requires()))
        

if __name__ == '__main__':
    luigi.build([Pipeline()], local_scheduler=True)