from clinical_trials.clinical_trials import Extract
from clinical_trials.clinical_trials import Load
from clinical_trials.clinical_trials import transform_study_fields

def main():

  # extract
  ct_extract = Extract(min_rank=1000, max_rank=1000)
  data = ct_extract.get_study_fields()

  # transform
  df = transform_study_fields(data)
  print(df.head())
  print(df.info())

  # load
  ct_load = Load()
  ct_load.load_study_fields(df)


if __name__ == '__main__':

  main()