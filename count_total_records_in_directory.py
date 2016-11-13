from concurrent.futures import ProcessPoolExecutor

e = ProcessPoolExecutor()


def p_count(filename,raw_data_directory = '../raw_data/',path_to_attachments = '../attachments/'):
    import pandas as pd
    path_to_csv = os.path.join(raw_data_directory, filename)
    df = pd.read_csv(path_to_csv)
    return len(df)

lengths = list(e.map(p_count,filenames))

print sum(lengths)