# AWS Utils
Utilities (Spring boot starters) for working with AWS services like SQS, SNS, DynamoDB

## General Information
- Project Name: aws-utils-starters
- GIT URL: <https://github.com/praveenpg/awsutils-starters>
- Slack Channel: user-services-scrum (Private Channel)

## Builds


## Requirements
- Java 8
- Maven

## Amazon Services Covered
- DynamoDB (https://aws.amazon.com/dynamodb/)
- SQS (https://aws.amazon.com/sqs/)
- SNS (https://aws.amazon.com/sns/)

## Getting Started

### SQS
- Create a spring boot project
- Add the following dependency
```xml
		<dependency>
			<groupId>org.awsutils</groupId>
			<artifactId>sqs-starter</artifactId>
			<version>1.0.1-beta11</version>
		</dependency>
```

Add the following to dependencyManagement
```xml
    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>software.amazon.awssdk</groupId>
                <artifactId>bom</artifactId>
                <version>2.20.126</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>
```

- Add `@EnableScheduling` to the main Application class.

- Add the following config in either application.yaml or application.properties
```yaml
org:
  awsutils:
    ratelimiter:
      config:
        elasticache:
          node: <elasticache-node>
        ratelimiters:
          test:
            maxRate: 25
            maxRateTimeUnit: SECONDS
            type: DISTRIBUTED  
    aws:
      region: us-east-1
      awsAccessKey: AccessKey
      awsAccessKeySecret: AccessKeySecret      
      sqs:
        common:
          threadPoolSize: 25
          useCommonThreadPool: false
          propertyReadType: PROPERTY

        listener:
          one:
            listenerName: Queue1Listener
            threadPoolSize: 20
            queueName: queue1Name
            rateLimiterName: test
            scheduleRunIntervalKey: queue1_listner_interval
          two:
            listenerName: Queue2Listener
            threadPoolSize: 20
            queueName: queue2Name
            rateLimiterName: test
            scheduleRunIntervalKey: queue2_listner_interval

queue1_listner_interval: 2000
queue2_listner_interval: 3000
```

- Pushing a message SQS: Pushing a message to SQS is as simple as creating a SQSMessage object and calling send method on it. Sample Code below
```java
final UserDataV1 sampleMessage = new UserDataV1("John", "Smith")
final SqsMessage<UserDataV1> sqsMessage = new SqsMessage<>("SAMPLE_MESSAGE_V1", sampleMessage);

sqsMessage.send(queue1Name);
```

- Receiving SQS Messages: Depending on the messageType defined in the SqsMessage instance, we will need a different
  MessageHandler for different types. The message handler will extend AbstractSqsMessageHandler.
  The logic for handling will go into the `execute` method. Sample code below.

```java
@MessageHandler(messageType = "SAMPLE_MESSAGE_V1")
public class SampleSqsMessageV1Handler extends AbstractSqsMessageHandler<UserDataV1>  {
    @Autowired
    @Qualifier("sampleServiceV1")
    private SampleService<UserDataV1> testService;

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleSqsMessageV1Handler.class);

    @Override
    public Mono<UserDataProcessResponse<UserDataV1>> execute(final UserDataV1 userDataV1) {
        LOGGER.info("in execute method of SampleSqsMessageV1Handler: " + userDataV1);
        return testService.handleUserData(userDataV1);
    }
}
```
### DynamoDb
- Add the following config in either application.yaml or application.properties
```yaml
org:
  awsutils:
    aws:
      region: us-east-1
      awsAccessKeySecret: HyGZ5Fs4gXEbHs5HBXHS5T53QyOTp4/7M8H1srmz
      awsAccessKey: AKIAX4VOLKE7E6H44UHZ
      ddb:
        entityBasePackage: com.example.entities
        repositoryBasePackage: com.example.repositories
```

- Create an entity class with the following annotation
```java
@DDBTable(name = "ddb-demo-user-info")
@Data
@AllArgsConstructor
public class UserInfo {
    @PK(type = PK.Type.HASH_KEY)
    @GlobalSecondaryIndex(name = "brand-emailAddress-index", type = PK.Type.RANGE_KEY, projectionType = GlobalSecondaryIndex.ProjectionType.KEYS_ONLY)
    private String emailAddress;

    @PK(type = PK.Type.RANGE_KEY)
    @GlobalSecondaryIndex(name = "brand-emailAddress-index", type = PK.Type.HASH_KEY, projectionType = GlobalSecondaryIndex.ProjectionType.KEYS_ONLY)
    private String brand;

    private String firstName;
    private String lastName;
    private String middleName;
    private String password;
}
```

- Add a repository class. The repository class needs to implement the BaseRepository interface.
- Querying by Hash Key and Range Key

```java
@DdbRepository(entityClass = UserInfo.class)
public class UserInfoRepository implements BaseRepository<UserInfo> {
    public Flux<UserInfo> getByEmailAddress(String emailAddress) {
        return findByHashKey("emailAddress", emailAddress);
    }

    public Flux<UserInfo> getByBrand(final String brand) {
        return findByGlobalSecondaryIndex("brand-emailAddress-index", brand);
    }
}
```
- Querying by Hash key and range key

```java
@Autowired
private UserInfoRepository userInfoRepository;
.
.
.
public Mono<UserInfo> findByEmailAddressAndBrand(final String emailAddress, final String brand) {
    return userInfoRepository.findByPrimaryKey(PrimaryKey.builder()
                                            .hashKeyName(getHashKeyName())
                                            .hashKeyValue(emailAddress)
                                            .rangeKeyName(getRangeKeyName())
                                            .rangeKeyValue(brand)
                                            .build());
}
.
.
.
```
- Querying by index

```java
@Autowired
private UserInfoRepository userInfoRepository;
.
.
.
public Mono<UserInfo> findByBrand(final String brand) {
    return userInfoRepository.findByGlobalSecondaryIndex("brand-emailAddress-index", brand);
}
.
.
.
```

- Example project: <Add example project here>

### RateLimiter
- Add the following config in either application.yaml or application.properties
```yaml
org:
  awsutils:
    ratelimiter:
      config:
        elasticache:
          node: dev-usersvccld-prefe-001.i2ztuy.0001.use1.cache.amazonaws.com
        ratelimiters:
          sample_service_testMethod:
            maxRate: 25
            maxRateTimeUnit: SECONDS
            type: DISTRIBUTED
          sample_service_testMethod2:
            maxRate: 25
            maxRateTimeUnit: SECONDS
            type: DISTRIBUTED
```
type: Allowed values:
- DISTRIBUTED (rate limited across JVMs)
- LOCAL (rate limited for JVM)

```java

```
Annotate the method that needs to be rate limited with the @RateLimited annotation.
The above method will now follow the following limits
type: DISTRIBUTED (rate is limited across all JVMS)
rate: 25 per second
