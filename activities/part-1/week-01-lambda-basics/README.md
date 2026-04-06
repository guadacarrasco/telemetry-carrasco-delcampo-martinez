# Week 1: Lambda Basics, IAM, and Environment Variables

## Objectives
- Understand AWS Lambda fundamentals (handler, event, context)
- Create a Lambda function that calls the OpenF1 API
- Configure IAM roles and environment variables
- Deploy using SAM CLI

## Tools
- AWS Console/CLI, Python 3.12, boto3, SAM CLI

## Activity
Create a `start_simulation` Lambda function that:
1. Reads a `SESSION_KEY` from environment variables
2. Calls the OpenF1 API to fetch session data
3. Returns the session information as JSON

## Steps

### 1. Review the starter code
Look at `starter/handler.py` — it has a Lambda handler skeleton with TODO comments.

### 2. Implement the handler
- Use the `requests` library to call `https://api.openf1.org/v1/sessions?session_key={SESSION_KEY}`
- Parse the response and return session info
- Handle errors (API failures, missing env vars)

### 3. Create the SAM template
- Define a `AWS::Serverless::Function` resource
- Set `Runtime: python3.12`, `Timeout: 30`, `MemorySize: 256`
- Add environment variable `SESSION_KEY`
- Configure an IAM role with basic Lambda execution permissions

### 4. Test locally
```bash
sam local invoke StartSimulation --env-vars env.json
```

### 5. Deploy
```bash
sam build
sam deploy --guided
```

## Expected Results
- Lambda returns JSON with session_key, session_name, circuit, dates
- CloudWatch logs show structured output
- Function executes in < 5 seconds

## Key Concepts
- **Lambda Handler**: `def handler(event, context)` — entry point for all invocations
- **Event**: Input data (from API Gateway, SQS, EventBridge, etc.)
- **Context**: Runtime info (function name, memory, timeout remaining)
- **IAM Role**: Permissions the Lambda function has to access AWS services
- **Environment Variables**: Configuration values injected at runtime

## Resources
- [AWS Lambda Developer Guide](https://docs.aws.amazon.com/lambda/latest/dg/)
- [OpenF1 API Documentation](https://openf1.org/)
- [SAM CLI Reference](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/)
