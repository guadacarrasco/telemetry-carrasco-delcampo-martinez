# Semana 5: Messaging — SQS y EventBridge

## Objetivos
- Comprender los patrones de colas de mensajes (point-to-point, pub/sub)
- Implementar un pipeline de eventos basado en SQS para telemetría
- Configurar EventBridge para disparar el motor del simulador en un horario definido
- Publicar y consumir mensajes desde SQS

## Herramientas
- CDK, SQS, EventBridge, Lambda

## Actividad
Construye el pipeline de eventos de telemetría:
1. El motor del simulador publica eventos de telemetría a una cola SQS
2. Una regla de EventBridge dispara el `engine_handler` cada 5 segundos
3. Un Lambda consumidor de eventos lee desde SQS y actualiza el estado en vivo

## Arquitectura
```
EventBridge (5s) → engine_handler → SQS (f1-telemetry-events) → event_consumer → DynamoDB
                                      ↓
                                    DLQ (3 retries)
```

## Pasos

### 1. Crear el stack de messaging
Define la cola SQS con DLQ, el Lambda consumidor (con `SqsEventSource`), el Lambda del motor y una regla de EventBridge que apunte al motor. **Nota:** `Schedule.rate()` de EventBridge requiere minutos enteros — usa `Duration.minutes(1)`, no segundos.

### 2. Implementar el servicio del simulador
Crea un servicio que genere eventos de telemetría aleatorios y los publique a SQS.

### 3. Implementar los handlers del motor y del consumidor
- `engine_handler.handler` — invocado por EventBridge, ejecuta un `SimulatorService.tick(...)`.
- `event_consumer.handler` — invocado por SQS, parsea cada record y hace upsert en `f1_live_state`. Devuelve `batchItemFailures` ante errores.

### 4. Desplegar el stack en LocalStack

La actividad es autocontenida — se despliega como su propio CDK app desde la carpeta `solution/`. Las colas pre-creadas por `init-aws.sh` colisionan con el stack, así que elimínalas primero.

```bash
# (una vez) iniciar LocalStack
cd localstack && docker compose up -d && cd ..

# Eliminar las colas pre-creadas para que el stack pueda crearlas
awslocal sqs delete-queue --queue-url http://localhost:4566/000000000000/f1-telemetry-events
awslocal sqs delete-queue --queue-url http://localhost:4566/000000000000/f1-telemetry-events-dlq

# Synth + deploy desde un directorio temporal (mantiene cdk.out fuera del asset)
mkdir -p /tmp/week5-cdk && cd /tmp/week5-cdk
cat > app.py <<'EOF'
import sys
sys.path.insert(0, "<repo>/activities/part-1/week-05-messaging/solution")
from aws_cdk import App
from messaging_stack import MessagingStack
app = App()
MessagingStack(app, "Week5Messaging")
app.synth()
EOF
echo '{"app": "python3 app.py"}' > cdk.json

cdklocal bootstrap
cdklocal deploy --require-approval never
```

### 5. Probar el pipeline
```bash
# Enviar un mensaje directamente a la cola — el Lambda consumidor debe drenarla
awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/f1-telemetry-events \
  --message-body '{"event_type":"telemetry_update","session_key":9468,"driver_number":1,"timestamp":"2024-01-01T00:00:00Z","data":{"speed":315,"rpm":12000,"throttle":80,"brake":0,"drs":1,"n_gear":7},"idempotency_key":"test-001"}'
sleep 5
awslocal dynamodb scan --table-name f1_live_state

# O invocar el motor manualmente (el mismo camino que tomaría EventBridge)
awslocal lambda invoke --function-name f1-engine-handler --payload '{}' /tmp/out.json
cat /tmp/out.json    # {"published": 4, "session_key": 9468}
```

## Conceptos clave
- **SQS**: Cola de mensajes administrada — desacopla productores y consumidores
- **Dead Letter Queue (DLQ)**: Captura mensajes que fallan en el procesamiento después de N reintentos
- **EventBridge Rule**: Dispara acciones en un horario definido o en respuesta a eventos
- **Visibility Timeout**: Tiempo que un mensaje permanece oculto para otros consumidores después de ser leído
- **Batch Processing**: Procesa múltiples mensajes de SQS en una sola invocación de Lambda

## Nota sobre AWS Academy
Kinesis generalmente no está disponible en cuentas academy. SQS ofrece capacidades de desacoplamiento similares para este caso de uso.
