Steps to initiate the services

1. Create docker network

       docker network create app-net

2. Run docker compose to start pulsar

       docker compose -f "docker-compose-pulsar.yml" up

      2.1   Run the following command from a cmd or powershell or hit the endpoints using a rest client to create the pulsar admin user

            CSRF_TOKEN=$(curl http://localhost:7750/pulsar-manager/csrf-token)
      
            curl \
            -H "X-XSRF-TOKEN: $CSRF_TOKEN" \
            -H "Cookie: XSRF-TOKEN=$CSRF_TOKEN;" \
            -H "Content-Type: application/json" \
            -X PUT http://localhost:7750/pulsar-manager/users/superuser \
            -d '{"name": "admin", "password": "apachepulsar", "description": "test", "email": "username@test.org"}'

      2.2
      Open up Pulsar Manager UI: http://localhost:9527

      Click on Create New Environment and create with following details

      environment name: spring-pulsar

      service url: http://pulsar:8080

      broker url : http://pulsar-broker:8080
   

3. Run docker compose to bring up orchestrator, service-a, service-b

       docker compose -f "docker-compose-services.yml" up --build

4. Hit the /orchestrate url from the orchestrator service

   http://localhost:8080/orchestrate
   method: POST

   request-
   Happy Path:

   {"serviceATag": "tagA", "serviceBTag": "tagB"}


############## Important URLs ###########

Orchestrator: http://localhost:8080

Service A: http://localhost:8081

Service B: http://localhost:8082

Pulsar Admin (standalone): http://localhost:8085

Pulsar Manager UI: http://localhost:9527
