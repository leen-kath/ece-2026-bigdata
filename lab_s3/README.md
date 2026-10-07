# Lab: Object storage with S3 — Answers

In this lab, I stored the datasets in an S3 bucket and I used a Kubernetes Job to upload them in the bronze layer.

## Ingestion Job

The file `job-upload-bronze.yaml` is the Job that uploads `users.csv` and `orders.csv` to `s3://<bucket>/bronze/`. It uses 3 objects that I created before:

- a ConfigMap `datasets` with the two CSV files, mounted in `/data`;
- a ConfigMap `s3-config` with the endpoint, the region and the bucket name;
- a Secret `s3-credentials` with the access keys.

At first, my Job was created but no pod started. With `kubectl describe job`, I saw in the events that the namespace has a quota (`onyxia-quota`) and every container must have `limits.cpu`. I added `cpu: 500m` and after that the Job worked.

## Answers to the questions

**1. Why are the credentials stored in a Secret and not in the ConfigMap?**

Because the credentials are sensitive data and the ConfigMap is made for normal configuration. With a Secret, we can give different permissions: someone can read the ConfigMaps without being able to read the Secrets. Also, `kubectl describe` shows the values of a ConfigMap, but for a Secret it only shows the size.

But a Secret is not really encrypted by default, it is only in base64, so anyone with the right permissions can decode it. So the protection depends mostly on the permissions of the cluster.

**2. The credentials are temporary. What happens if the Job runs again tomorrow? How would a production platform give credentials to a Job?**

The Secret has a copy of my credentials from today. Tomorrow the session token will be expired, so the upload will fail with an authentication error (like `ExpiredToken`). The Job will try again 2 times because of `backoffLimit: 2`, and then it will fail.

In production, I think it is not a good idea to copy credentials by hand. The Job should have its own identity, for example with a service account that is linked to a role with access to S3, and the credentials are renewed automatically. Another solution is a tool that manages the secrets and updates them automatically.

**3. How would you turn this Job into a daily ingestion?**

I would use a CronJob. It is like a Job, but it runs on a schedule, like `cron` on Linux. For example, to run it every day at 2 am:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: upload-bronze-daily
spec:
  schedule: "0 2 * * *"
  jobTemplate:
    spec:
      # same content as the Job
```

But it is not enough alone. The credentials need to be renewed (question 2), the data should come from the real source and not from a ConfigMap, and the file names should have the date (for example `bronze/users/date=2026-09-28/users.csv`) so we don't overwrite the data of the day before.
