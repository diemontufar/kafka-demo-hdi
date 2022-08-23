# kafka-demo-hdi

### 1. Build Projects

Repeat this process for all dotnet projects.

```
dotnet build
```

### 2. Follow prerequisites 

[Configure SSL trust store](https://docs.confluent.io/platform/current/tutorials/examples/clients/docs/csharp.html#client)

### 3. Add API username and password

Edit csharp.config and confluent_python.config files and change this two parameters with your Confluent API key credentials:

```
sasl.username=<confluent_api_user_name>
sasl.password=<confluent_api_user_pass>
```

### 3. Run Producer

```
cd ./UnoCabinaProducer
dotnet run produce siniestros ./csharp.config
```

### 4. Run Consumers

```
cd ./AjustadorAutoConsumer
dotnet run ajustador-auto siniestros ~/.confluent/csharp.config
```

```
cd ./AjustadorDanoConsumer
dotnet run ajustador-dano siniestros ~/.confluent/csharp.config
```

```
cd ./AnalistaDanoConsumer
dotnet run analista-dano siniestros ~/.confluent/csharp.config
```

### 5. Run Power BI consumer

```
cd ./PowerBIConsumer
python powerbi_consumer.py -f ./confluent_python.config -t siniestros
```