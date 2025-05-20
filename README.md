


psql model_utils -c "select * from training"





1. caputre git commit in trainig job
2. check mlflow 3.0 example for gettign evals in same run.  using CV. 
3. sql statement (Or automl style check boxes to eliminate features)
4. Add eval notebook like (https://e2-demo-field-eng.cloud.databricks.com/editor/notebooks/990384398408388?o=1444828305810485#command/990384398409470)


5. reconsider workflow.    report cross-validation results from training run.   Use hyper-parements (depth) discovered from cv to train model on full data set.  Register model.  apply eval using test data set.  Connect train and eval runs using model and version?  [yes, this is connected through the model or experiement]




split traing and eval into separate notebooks?  or make a separate eval notebook available

i need an integration test tab...based on model version + a data set.  what is the data set and where is it defined?  Just use eval dataset?  Remove label?  or have an integration tab.


I have cv now which doens't use eval dataset.  But...I either need to disallo


notebooks/01_Train_Regression_Model.py