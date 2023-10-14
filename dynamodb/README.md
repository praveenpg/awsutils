# dynamodb-data
An Object model (almost) for dynamoDb

## General Information
- Project Name: DynamoDB Data\
  This project attempts to develop an Object model for AWS DynamoDB database based systems. Entities are developed as POJOs with annotations as shown in the examples below.
- GIT URL: <https://github.com/praveenpg/awsutils-starters>
## Requirements
- Java 11
- Maven

## Amazon Services Covered
- DynamoDB (https://aws.amazon.com/dynamodb/)

## Getting Started
#### Maven Dependency

```xml
        <dependency>
            <groupId>org.awsutils</groupId>
            <artifactId>dynamodb-starter</artifactId>
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

- Add the following config in either application.yaml or application.properties
```yaml
org:
  leo:
    aws:
      region: us-east-1
      ddb:
        entity-base-package: org.awsutils.ddbdemo.entities
        repository-base-package: org.awsutils.ddbdemo.dao
      aws-access-key: <access_key>
      aws-access-key-secret: <secret>
```
- Create an entity class with the following annotation. All entity classes should have an empty constructor. It can be private.
```java
@DDBTable(name = "ddb-demo-user-info")
@Data //Lombok annotation (Optional)
@AllArgsConstructor //Lombok annotation (Optional)
public class UserInfo {
    @HashKey
    @SecondaryIndex(name = "division-emailAddress-index", type = KeyType.RANGE_KEY, projectionType = ProjectionType.KEYS_ONLY)
    private String emailAddress;
    @RangeKey
    @SecondaryIndex(name = "division-emailAddress-index", type = KeyType.HASH_KEY, projectionType = ProjectionType.KEYS_ONLY)
    private String division;
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
public class UserInfoRepository implements NonBlockingBaseRepository<UserInfo> {
    public Flux<UserInfo> getByEmailAddress(String emailAddress) {
        return findByHashKey("emailAddress", emailAddress);
    }
    public Flux<UserInfo> getByDivision(final String division) {
        return findByGlobalSecondaryIndex("division-emailAddress-index", division);
    }
}

```
- Querying by Hash key and range key
```java
class UserService {
@Autowired
private UserInfoRepository userInfoRepository;

    public Mono<UserInfo> findByEmailAddressAndDivision(final String emailAddress, final String division) {
        return userInfoRepository.findByPrimaryKey(PrimaryKey.builder()
                                                .hashKeyName(userInfoRepository.getHashKeyName())
                                                .hashKeyValue(emailAddress)
                                                .rangeKeyName(userInfoRepository.getRangeKeyName())
                                                .rangeKeyValue(division)
                                                .build());
    }
}
```

- Querying by index
```java
class UserService {
    @Autowired
    private UserInfoRepository userInfoRepository;
    
    public Flux<UserInfo> findByDivision(final String division) {
        return userInfoRepository.findByGlobalSecondaryIndex("division-emailAddress-index", division);
    }
}
```
- Using Filter Expressions
  The library also supports using filter expression. Example below:
```java
@Data //Lombok
@DDBTable(name = "${sub.category.table.name}")
public class SubCategory extends VersionedEntity {
  @HashKey
  @DbAttribute("subCategoryId")
  @SecondaryIndex(name = "categoryId-subCategoryId-index", type = KeyType.RANGE_KEY, projectionType = ProjectionType.ALL)
  private String id;

  @SecondaryIndex(name = "categoryId-subCategoryId-index", type = KeyType.HASH_KEY, projectionType = ProjectionType.ALL)
  private String categoryId;

  @DbAttribute(nullable = false)
  private String name;

  @DbAttribute(nullable = false)
  private String description;
}
```
If we would like to query the above to fetch sub categories which does match a particular criteriaId and whose versions fall between 0 and 4, we can do something like shown below:
```java
    public Flux<SubCategory> findSubCategoriesByCategoryId(final String categoryId, final String name) {
        return subCategoryRepository.findBySecondaryIndex("categoryId-subCategoryId-index", categoryId,
                Expr.builder()
                        .name("name", "xyz")
                        .ne()
                        .value("name", AttributeValue.builder().s(name).build())
                        .and()
                        .name("version", "ver")
                        .between()
                        .value("lv", AttributeValue.builder().n("-1").build(), "hv", AttributeValue.builder().n("4").build())
                        .buildFilterExpression());
    }
```
The above would be the equivalent of
```java
aws dynamodb query \
    --table-name sub-category \
    --key-condition-expression "ForumName = :name and Subject = :sub" \
    --expression-attribute-values  file://values.json
```