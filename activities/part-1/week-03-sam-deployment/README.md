# Week 3: SAM Deep Dive — Deployment and Configuration

## Objectives
- Master SAM CLI commands: build, deploy, local invoke, local start-api
- Parameterize templates for multi-environment deployment (dev/prod)
- Configure function memory, timeout, and concurrency
- Use samconfig.toml for deployment profiles

## Tools
- SAM CLI, AWS CloudFormation

## Activity
Migrate the Week 2 template into a production-ready SAM project with:
1. Parameters for Environment (dev/prod)
2. Globals for shared function config
3. samconfig.toml with deployment profiles
4. Local testing with `sam local invoke` and `sam local start-api`

## Steps

### 1. Parameterize the template
- Add `Environment` parameter (dev/prod)
- Use `!Sub` to create environment-specific resource names
- Add `Condition` for production-specific settings

### 2. Configure samconfig.toml
- Create profiles for dev and prod
- Set stack name, region, capabilities

### 3. Test locally
```bash
sam build
sam local invoke ListSessionsFunction
sam local start-api
curl http://localhost:3000/sessions
```

### 4. Deploy to multiple environments
```bash
sam deploy --config-env dev
sam deploy --config-env prod
```

## Key Concepts
- **SAM Build**: Packages code and dependencies
- **Parameters**: Template inputs for environment-specific config
- **Globals**: Shared settings across all functions
- **samconfig.toml**: Deployment configuration profiles
- **sam local**: Local testing without deploying to AWS
