# Semana 3: SAM a Fondo — Deployment y Configuración

## Objetivos
- Dominar los comandos de SAM CLI: build, deploy, local invoke, local start-api
- Parametrizar templates para deployment multi-entorno (dev/prod)
- Configurar memoria, timeout y concurrencia de funciones
- Usar samconfig.toml para perfiles de deployment

## Herramientas
- SAM CLI, AWS CloudFormation

## Actividad
Migra el template de la Semana 2 a un proyecto SAM listo para producción con:
1. Parameters para Environment (dev/prod)
2. Globals para configuración compartida de funciones
3. samconfig.toml con perfiles de deployment
4. Pruebas locales con `sam local invoke` y `sam local start-api`

## Pasos

### 1. Parametriza el template
- Agrega un parameter `Environment` (dev/prod)
- Usa `!Sub` para crear nombres de recursos específicos por entorno
- Agrega una `Condition` para configuraciones exclusivas de producción

### 2. Configura samconfig.toml
- Crea perfiles para dev y prod
- Define stack name, region y capabilities

### 3. Prueba localmente
```bash
sam build
sam local invoke ListSessionsFunction
sam local start-api
curl http://localhost:3000/sessions
```

### 4. Despliega en múltiples entornos
```bash
sam deploy --config-env dev
sam deploy --config-env prod
```

## Conceptos clave
- **SAM Build**: Empaqueta el código y sus dependencias
- **Parameters**: Entradas del template para configuración específica por entorno
- **Globals**: Configuración compartida entre todas las funciones
- **samconfig.toml**: Perfiles de configuración de deployment
- **sam local**: Pruebas locales sin necesidad de desplegar en AWS
