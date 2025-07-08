from fastapi import FastAPI, Query
import subprocess
import os

app = FastAPI()

@app.get("/run")
def run_job(file: str = Query(..., description="Path to Spark job file")):
    # Đường dẫn tuyệt đối
    spark_file_path = os.path.join("/opt/bitnami/spark", file)

    if not os.path.exists(spark_file_path):
        return {"error": f"File {spark_file_path} not found."}

    try:
        result = subprocess.run([
            "/opt/bitnami/spark/bin/spark-submit",
            "--master", "local",
            "--deploy-mode", "client",
            spark_file_path
        ], capture_output=True, text=True, timeout=120)

        return {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "exit_code": result.returncode
        }

    except subprocess.TimeoutExpired:
        return {"error": "Spark job timed out."}
    except Exception as e:
        return {"error": str(e)}

    
@app.get("/health")
def health_check():
    """
    Health check endpoint to verify if the API is running.
    """
    return {"status": "ok"}