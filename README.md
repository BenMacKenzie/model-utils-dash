Next step: integrating with model training.

1. add github repo to project.
2. add training tab.  List dataset info selected on previous tab.  create a generic 'parameters' string to pass to training job.  1-M relationship between datasets and models.  list associated experiments.  set run name based on time and dataset?  create a table to track experiments associated with a project: project_id, dataset_id, run_id (how do we associated evaluation and training), how do we assoicate jobs?



psql model_utils -c "select * from training"

need to include dataset and/or job id in mflow run so that we can track which runs belong to which dataset. also should log a dataset.  [need to update training notebook.]

split traing and eval into separate notebooks?  or make a separate eval notebook available

