import mlflow
import os
from datetime import datetime

def log_to_mlflow(params=None, metrics=None, artifacts=None):
    mlflow.start_run()
    
    # Create a timestamp string
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Log parameters
    if params:
        params_file = f"auto_trigger/logger/logs/params_{timestamp}.txt"
        with open(params_file, "w") as f:
            for key, value in params.items():
                f.write(f"{key}: {value}\n")
        mlflow.log_artifact(params_file, "params")
    
    # Log metrics
    if metrics:
        metrics_file = f"auto_trigger/logger/logs/metrics_{timestamp}.txt"
        with open(metrics_file, "w") as f:
            for key, value in metrics.items():
                f.write(f"{key}: {value}\n")
        mlflow.log_artifact(metrics_file, "metrics")
        
    # Log artifacts
    if artifacts:
        artifact_dir = f"auto_trigger/logger/logs/artifacts_{timestamp}"
        os.makedirs(artifact_dir, exist_ok=True)
        for artifact_path in artifacts:
            filename = os.path.basename(artifact_path)
            mlflow.log_artifact(artifact_path, artifact_dir, artifact_path=filename)
        
    mlflow.end_run()
