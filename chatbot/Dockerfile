# Hugging Face Docker Space for the portfolio assistant.
# Spaces run the container as user 1000 and route traffic to app_port (7860).
FROM python:3.12-slim

RUN useradd -m -u 1000 user
USER user
ENV HOME=/home/user PATH=/home/user/.local/bin:$PATH
WORKDIR /home/user/app

COPY --chown=user requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

COPY --chown=user app.py retrieval.py ./
COPY --chown=user knowledge/chunks.jsonl knowledge/chunks.jsonl

EXPOSE 7860
CMD ["streamlit", "run", "app.py", "--server.port=7860", "--server.address=0.0.0.0", \
     "--server.headless=true", "--browser.gatherUsageStats=false"]
