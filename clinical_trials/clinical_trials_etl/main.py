from clinical_trials import Extract
from clinical_trials import Load
from clinical_trials import transform_study_fields

def main():

  # extract
  ct_extract = Extract()
  data = ct_extract.get_study_fields()

  # transform
  df = transform_study_fields(data)
  print(df)

  # load
  ct_load = Load()
  ct_load.load_study_fields(df)
  ct_load.write_study_fields(df)


if __name__ == '__main__':

  main()