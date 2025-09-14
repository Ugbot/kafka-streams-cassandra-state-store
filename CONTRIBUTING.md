<!-- omit in toc -->
# Contributing to kafka-streams-cassandra-state-store

First off, thanks for taking the time to contribute! ❤️

All types of contributions are encouraged and valued. See the [Table of Contents](#table-of-contents) for different ways to help and details about how this project handles them. Please make sure to read the relevant section before making your contribution. It will make it a lot easier for us maintainers and smooth out the experience for all involved. The community looks forward to your contributions. 🎉

> And if you like the project, but just don't have time to contribute, that's fine. There are other easy ways to support the project and show your appreciation, which we would also be very happy about:
> - Star the project
> - Tweet about it
> - Refer this project in your project's readme
> - Mention the project at local meetups and tell your friends/colleagues

<!-- omit in toc -->
## Table of Contents

- [I Have a Question](#i-have-a-question)
- [I Want To Contribute](#i-want-to-contribute)
- [Reporting Bugs](#reporting-bugs)
- [Suggesting Enhancements](#suggesting-enhancements)
- [Your First Code Contribution](#your-first-code-contribution)
- [Improving The Documentation](#improving-the-documentation)
- [Styleguides](#styleguides)
- [Commit Messages](#commit-messages)
- [Join The Project Team](#join-the-project-team)



## I Have a Question

> If you want to ask a question, we assume that you have read the available [Documentation](/README.md).

Before you ask a question, it is best to search for existing [Issues](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/issues) that might help you. In case you have found a suitable issue and still need clarification, you can write your question in this issue. It is also advisable to search the internet for answers first.

If you then still feel the need to ask a question and need clarification, we recommend the following:

- Open an [Issue](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/issues/new).
- Provide as much context as you can about what you're running into.
- Provide project and platform versions (nodejs, npm, etc), depending on what seems relevant.

We will then take care of the issue as soon as possible.

<!--
You might want to create a separate issue tag for questions and include it in this description. People should then tag their issues accordingly.

Depending on how large the project is, you may want to outsource the questioning, e.g. to Stack Overflow or Gitter. You may add additional contact and information possibilities:
- IRC
- Slack
- Gitter
- Stack Overflow tag
- Blog
- FAQ
- Roadmap
- E-Mail List
- Forum
-->

## I Want To Contribute

> ### Legal Notice <!-- omit in toc -->
> When contributing to this project, you must agree that you have authored 100% of the content, that you have the necessary rights to the content and that the content you contribute may be provided under the project license.

### Reporting Bugs

<!-- omit in toc -->
#### Before Submitting a Bug Report

A good bug report shouldn't leave others needing to chase you up for more information. Therefore, we ask you to investigate carefully, collect information and describe the issue in detail in your report. Please complete the following steps in advance to help us fix any potential bug as fast as possible.

- Make sure that you are using the latest version.
- Determine if your bug is really a bug and not an error on your side e.g. using incompatible environment components/versions (Make sure that you have read the [documentation](/README.md). If you are looking for support, you might want to check [this section](#i-have-a-question)).
- To see if other users have experienced (and potentially already solved) the same issue you are having, check if there is not already a bug report existing for your bug or error in the [bug tracker](https://github.com/thriving-dev/kafka-streams-cassandra-state-storeissues?q=label%3Abug).
- Also make sure to search the internet (including Stack Overflow) to see if users outside of the GitHub community have discussed the issue.
- Collect information about the bug:
- Stack trace (Traceback)
- OS, Platform and Version (Windows, Linux, macOS, x86, ARM)
- Version of the interpreter, compiler, SDK, runtime environment, package manager, depending on what seems relevant.
- Possibly your input and the output
- Can you reliably reproduce the issue? And can you also reproduce it with older versions?

<!-- omit in toc -->
#### How Do I Submit a Good Bug Report?

> You must never report security related issues, vulnerabilities or bugs including sensitive information to the issue tracker, or elsewhere in public. Instead sensitive bugs must be sent by email to <>.
<!-- You may add a PGP key to allow the messages to be sent encrypted as well. -->

We use GitHub issues to track bugs and errors. If you run into an issue with the project:

- Open an [Issue](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/issues/new). (Since we can't be sure at this point whether it is a bug or not, we ask you not to talk about a bug yet and not to label the issue.)
- Explain the behavior you would expect and the actual behavior.
- Please provide as much context as possible and describe the *reproduction steps* that someone else can follow to recreate the issue on their own. This usually includes your code. For good bug reports you should isolate the problem and create a reduced test case.
- Provide the information you collected in the previous section.

Once it's filed:

- The project team will label the issue accordingly.
- A team member will try to reproduce the issue with your provided steps. If there are no reproduction steps or no obvious way to reproduce the issue, the team will ask you for those steps and mark the issue as `needs-repro`. Bugs with the `needs-repro` tag will not be addressed until they are reproduced.
- If the team is able to reproduce the issue, it will be marked `needs-fix`, as well as possibly other tags (such as `critical`), and the issue will be left to be [implemented by someone](#your-first-code-contribution).

<!-- You might want to create an issue template for bugs and errors that can be used as a guide and that defines the structure of the information to be included. If you do so, reference it here in the description. -->


### Suggesting Enhancements

This section guides you through submitting an enhancement suggestion for kafka-streams-cassandra-state-store, **including completely new features and minor improvements to existing functionality**. Following these guidelines will help maintainers and the community to understand your suggestion and find related suggestions.

<!-- omit in toc -->
#### Before Submitting an Enhancement

- Make sure that you are using the latest version.
- Read the [documentation](/README.md) carefully and find out if the functionality is already covered, maybe by an individual configuration.
- Perform a [search](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/issues) to see if the enhancement has already been suggested. If it has, add a comment to the existing issue instead of opening a new one.
- Find out whether your idea fits with the scope and aims of the project. It's up to you to make a strong case to convince the project's developers of the merits of this feature. Keep in mind that we want features that will be useful to the majority of our users and not just a small subset. If you're just targeting a minority of users, consider writing an add-on/plugin library.

<!-- omit in toc -->
#### How Do I Submit a Good Enhancement Suggestion?

Enhancement suggestions are tracked as [GitHub issues](https://github.com/thriving-dev/kafka-streams-cassandra-state-store/issues).

- Use a **clear and descriptive title** for the issue to identify the suggestion.
- Provide a **step-by-step description of the suggested enhancement** in as many details as possible.
- **Describe the current behavior** and **explain which behavior you expected to see instead** and why. At this point you can also tell which alternatives do not work for you.
- You may want to **include screenshots and animated GIFs** which help you demonstrate the steps or point out the part which the suggestion is related to. You can use [this tool](https://www.cockos.com/licecap/) to record GIFs on macOS and Windows, and [this tool](https://github.com/colinkeenan/silentcast) or [this tool](https://github.com/GNOME/byzanz) on Linux. <!-- this should only be included if the project has a GUI -->
- **Explain why this enhancement would be useful** to most kafka-streams-cassandra-state-store users. You may also want to point out the other projects that solved it better and which could serve as inspiration.

<!-- You might want to create an issue template for enhancement suggestions that can be used as a guide and that defines the structure of the information to be included. If you do so, reference it here in the description. -->

### Your First Code Contribution

#### Development Environment Setup

1. **Prerequisites**
   - Java 17+ (we use modern Java features)
   - Docker & Docker Compose (for testing)
   - Git
   - Your favorite IDE (IntelliJ IDEA recommended for modern Java support)

2. **Clone and Setup**
   ```bash
   git clone https://github.com/thriving-dev/kafka-streams-cassandra-state-store.git
   cd kafka-streams-cassandra-state-store
   ./gradlew build  # This will download all dependencies
   ```

3. **IDE Configuration**
   - Set Java SDK to Java 17+
   - Enable modern Java language features
   - Configure code style for modern Java (text blocks, records, etc.)

#### Understanding the Architecture

##### Core Components
- **`CassandraSchema`**: Schema definition with modern records
- **`ColumnDefinition`**: Immutable column metadata
- **`SchemaValidation`**: Runtime validation logic
- **`TypedCassandraRepository`**: Type-safe repository implementations
- **`CassandraStores.Builder`**: Fluent API for store creation

##### Modern Java Features Used
- **Records** for immutable data structures
- **Text Blocks** for readable SQL generation
- **Switch Expressions** for type mapping
- **Pattern Matching** for validation
- **Stream API** for functional data processing

#### Development Workflow

1. **Create a Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Write Tests First** (TDD approach)
   ```java
   // Example: Test for new schema template
   @Test
   void shouldCreateValidProductCatalogSchema() {
       CassandraSchema schema = SchemaTemplates.productCatalogSchema("products");

       assertThat(schema.getTableName()).isEqualTo("products");
       assertThat(schema.hasColumn("product_id")).isTrue();
       // ... more assertions
   }
   ```

3. **Implement the Feature**
   - Follow modern Java patterns
   - Use records for immutable data
   - Use text blocks for multi-line strings
   - Use switch expressions where applicable

4. **Run Tests**
   ```bash
   # Run unit tests
   ./gradlew test

   # Run integration tests (requires Docker)
   ./gradlew intTest

   # Run specific test class
   ./gradlew test --tests "*SchemaValidationTest*"
   ```

5. **Update Documentation**
   - Update README.md if adding new features
   - Add Javadoc comments for public APIs
   - Update examples if needed

#### Working with Schema-Aware Features

##### Adding a New Schema Template
```java
// In SchemaTemplates.java
public static CassandraSchema analyticsSchema(String tableName) {
    return CassandraSchema.builder(tableName)
            .addPartitionKeyColumn("tenant_id", "uuid")
            .addPartitionKeyColumn("metric_name", "text")
            .addClusteringKeyColumn("timestamp", "timestamp")
            .addColumn("value", "double")
            .addColumn("dimensions", "map<text, text>")
            .addColumn("tags", "set<text>")
            .build();
}
```

##### Adding New CQL Type Support
```java
// In TypeMapping.java
private static void initializeMappings() {
    // Add your custom type mapping
    mapType(YourCustomType.class, "frozen<your_type>", DataTypes.custom("com.example.YourType"));
}
```

##### Testing Schema Validation
```java
@Test
void shouldValidateSchemaCompatibility() {
    CassandraSchema sourceSchema = createSourceSchema();
    CassandraSchema targetSchema = createTargetSchema();

    ValidationResult result = SchemaValidation.validateSchemaCompatibility(sourceSchema, targetSchema);

    assertThat(result.valid()).isTrue();
    assertThat(result.errors()).isEmpty();
}
```

#### Docker Testing Guidelines

##### Running Integration Tests
```bash
# Start test containers
docker-compose -f examples/schema-aware-word-count/docker-compose.yml up -d

# Run integration tests
./gradlew intTest --tests "*TypedSchemaStorePerformanceTest*"

# Clean up
docker-compose -f examples/schema-aware-word-count/docker-compose.yml down
```

##### Testcontainers Configuration
```java
@Container
private static final CassandraContainer cassandra = new CassandraContainer("cassandra:4.1")
        .withExposedPorts(9042)
        .withInitScript("schema.cql");

@Container
private static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
        .withEmbeddedZookeeper();
```

#### Code Style Guidelines

##### Modern Java Patterns
- Use **records** for immutable data classes
- Use **text blocks** for multi-line strings
- Use **switch expressions** instead of switch statements
- Use **pattern matching instanceof** where applicable
- Prefer **streams** over traditional loops

##### Example: Modern Java Implementation
```java
// ✅ Modern approach
public record ColumnDefinition(
        String name,
        String cqlType,
        boolean isPrimaryKey,
        boolean isPartitionKey,
        boolean isClusteringKey
) {
    public ColumnDefinition {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Column name cannot be null or blank");
        }
    }
}

// ❌ Avoid old patterns
public class ColumnDefinition {
    private final String name;
    private final String cqlType;
    // ... 50+ lines of boilerplate
}
```

##### Naming Conventions
- Use `camelCase` for methods and variables
- Use `PascalCase` for classes and records
- Use descriptive names: `wordCountSchema` not `wcs`
- Use `isValid()`, `hasColumn()` for boolean methods
- Use `getColumn()`, `createSchema()` for getters/factories

#### Testing Best Practices

##### Unit Testing Schema Features
```java
@Test
void shouldCreateValidSchemaWithAllColumnTypes() {
    CassandraSchema schema = CassandraSchema.builder("test_table")
            .addPartitionKeyColumn("pk", "uuid")
            .addClusteringKeyColumn("ck", "timestamp")
            .addColumn("text_col", "text")
            .addColumn("int_col", "int")
            .addColumn("bool_col", "boolean")
            .addColumn("decimal_col", "decimal")
            .build();

    assertThat(schema.getColumns()).hasSize(6);
    assertThat(schema.getPrimaryKeyColumns()).hasSize(2);
    assertThat(schema.getRegularColumns()).hasSize(4);
}
```

##### Integration Testing Schema Validation
```java
@Test
void shouldValidateTopicAgainstSchema() {
    // Given
    CassandraSchema schema = SchemaTemplates.wordCountSchema("word_counts");
    String topicName = "word-input";

    // Produce test data
    produceTestData(topicName);

    // When
    ValidationResult result = TopicSchemaValidator.validateTopic(
            consumer, topicName, schema, keyDeserializer, valueDeserializer, 100);

    // Then
    assertThat(result.valid()).isTrue();
}
```

##### Performance Testing
```java
@Test
void shouldPerformQueriesFasterWithSchemaAwareness() {
    // Setup
    CassandraSchema schema = SchemaTemplates.wordCountSchema("perf_test");
    KeyValueStore<String, Long> typedStore = createTypedStore(schema);
    KeyValueStore<Bytes, byte[]> blobStore = createBlobStore();

    // Populate with test data
    populateTestData(typedStore, blobStore, 10000);

    // Benchmark typed store
    long typedTime = benchmarkQuery(() -> typedStore.get("test_word"));

    // Benchmark blob store
    long blobTime = benchmarkQuery(() -> blobStore.get(Bytes.wrap("test_word".getBytes())));

    // Assert performance improvement
    assertThat(typedTime).isLessThan(blobTime);
}
```

#### Contributing Schema Templates

##### Template Design Guidelines
- **Use meaningful names**: `userProfileSchema`, not `ups`
- **Include all common fields**: partition keys, timestamps, etc.
- **Document field purposes**: Javadoc for each field
- **Provide examples**: Usage examples in doc comments
- **Test thoroughly**: Unit tests for each template

##### Example Template Contribution
```java
/**
 * Creates a schema for e-commerce product data.
 * Includes catalog information, pricing, and inventory.
 *
 * <p>Example usage:
 * <pre>{@code
 * CassandraSchema productSchema = SchemaTemplates.productCatalogSchema("products");
 * KeyValueStore<String, Product> store = CassandraStores.builder(session, "products")
 *     .withSchema(productSchema)
 *     .partitionedKeyValueStore();
 * }</pre>
 */
public static CassandraSchema productCatalogSchema(String tableName) {
    return CassandraSchema.builder(tableName)
            .addPartitionKeyColumn("category", "text")
            .addClusteringKeyColumn("product_id", "uuid")
            .addColumn("name", "text")
            .addColumn("description", "text")
            .addColumn("price", "decimal")
            .addColumn("currency", "text")
            .addColumn("inventory_count", "int")
            .addColumn("tags", "set<text>")
            .addColumn("created_at", "timestamp")
            .addColumn("updated_at", "timestamp")
            .build();
}
```

### Improving The Documentation

#### Documentation Areas That Need Attention

##### README.md Enhancements
- **API Reference Section**: Add detailed method signatures and parameter descriptions
- **Troubleshooting Guide**: Common issues and solutions
- **Performance Tuning**: Advanced configuration options
- **Migration Cookbook**: Step-by-step migration recipes

##### Javadoc Improvements
- **Complete Coverage**: Ensure all public APIs have comprehensive Javadoc
- **Usage Examples**: Include code examples in method documentation
- **Performance Notes**: Document performance characteristics
- **Thread Safety**: Document concurrency guarantees

##### Example Enhancements
- **Complete Applications**: Full end-to-end examples
- **Configuration Variations**: Different deployment scenarios
- **Monitoring Integration**: How to integrate with monitoring systems

#### Documentation Standards

##### README Structure
```
## Overview
## 🚀 Schema-Aware State Stores (New!)
## Usage
## 📚 API Reference
## 🔧 Configuration
## 🚀 Performance & Tuning
## 🔄 Migration Guide
## 🐛 Troubleshooting
## 🤝 Contributing
## 📄 License
```

##### Code Example Standards
- **Copy-paste ready**: All examples should work as-is
- **Gradually complex**: Start simple, build to advanced
- **Well-commented**: Explain each step
- **Error handling**: Include proper exception handling
- **Best practices**: Follow established patterns

##### Javadoc Standards
```java
/**
 * Validates that a topic conforms to the specified schema.
 *
 * <p>This method performs comprehensive validation by sampling records
 * from the topic and checking them against the schema definition.
 * Validation includes type compatibility, required fields, and
 * data format correctness.</p>
 *
 * <p>Example usage:
 * <pre>{@code
 * ValidationResult result = TopicSchemaValidator.validateTopic(
 *     consumer, "user-events", userSchema,
 *     stringDeserializer, userDeserializer, 1000);
 *
 * if (!result.valid()) {
 *     throw new IllegalStateException("Schema validation failed: " +
 *         result.message());
 * }
 * }</pre></p>
 *
 * @param consumer the Kafka consumer to use for sampling
 * @param topicName the name of the topic to validate
 * @param schema the schema to validate against
 * @param keyDeserializer deserializer for message keys
 * @param valueDeserializer deserializer for message values
 * @param sampleSize number of records to sample (recommended: 100-1000)
 * @return validation result with success/failure and error details
 * @throws IllegalArgumentException if any parameter is null or invalid
 * @since 2.0.0
 */
public static ValidationResult validateTopic(...)
```

### Styleguides

#### Code Style Guidelines

##### Java Language Features
- **Java 17+**: Target modern Java features
- **Records**: Use for immutable data classes
- **Text Blocks**: Use for multi-line strings
- **Switch Expressions**: Prefer over switch statements
- **Pattern Matching**: Use instanceof with pattern variables
- **Streams API**: Prefer functional style over imperative loops

##### Naming Conventions
- **Classes**: `PascalCase` (e.g., `CassandraSchema`, `ColumnDefinition`)
- **Methods**: `camelCase` (e.g., `getColumn()`, `validateSchema()`)
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `DEFAULT_TABLE_NAME_FN`)
- **Packages**: `lowercase` (e.g., `dev.thriving.oss.kafka.streams.cassandra.state.store`)
- **Test Methods**: `shouldDoSomething` or `testSomething`

##### File Organization
```
src/main/java/
├── schema/           # Schema-related classes
├── repo/             # Repository implementations
│   └── typed/        # Type-safe implementations
├── utils/            # Utility classes
└── examples/         # Example applications

src/test/java/        # Unit tests
src/intTest/java/     # Integration tests
```

#### Commit Messages

##### Format
```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

##### Types
- **feat**: New feature
- **fix**: Bug fix
- **docs**: Documentation changes
- **style**: Code style changes (formatting, etc.)
- **refactor**: Code refactoring
- **test**: Adding or fixing tests
- **chore**: Maintenance tasks

##### Examples
```bash
feat(schema): add support for custom CQL types
fix(validation): handle null values in schema validation
docs(readme): update migration guide with examples
test(schema): add performance tests for typed stores
refactor(repo): modernize repository with records and streams
```

##### Scope Guidelines
- **schema**: Schema-related changes
- **repo**: Repository implementations
- **validation**: Validation logic
- **store**: Store implementations
- **test**: Testing infrastructure
- **docs**: Documentation
- **build**: Build system changes

### Commit Messages

#### Good Commit Messages
```bash
feat(schema): add support for frozen collections in schema templates

- Add frozen<list<type>> and frozen<set<type>> support
- Update TypeMapping to handle frozen collections
- Add tests for frozen collection validation

Closes #123
```

#### Bad Commit Messages
```bash
fix bug
update code
add stuff
```

#### Commit Message Guidelines
- **Subject line**: 50 characters or less
- **Body**: Explain what and why, not how
- **Footer**: Reference issues with "Closes #123"
- **Imperative mood**: "Add feature" not "Added feature"
- **Separate concerns**: One logical change per commit

### Schema Design Guidelines

#### Column Naming
- Use `snake_case` for Cassandra column names
- Use `camelCase` for Java field names
- Be descriptive: `user_id` not `uid`, `created_at` not `ts`

#### Primary Key Design
- **Partition Keys**: Distribute data evenly across nodes
- **Clustering Keys**: Enable efficient range queries
- **Composite Keys**: Balance distribution and query patterns

#### Data Type Selection
- **Text**: For variable-length strings
- **UUID**: For unique identifiers
- **Timestamp**: For time-based data
- **Decimal**: For precise financial calculations
- **Collections**: For variable-length lists/sets/maps

#### Schema Evolution
- **Additive Changes**: Safe to add new columns
- **Compatible Types**: Can change int to bigint, text to varchar
- **Breaking Changes**: Require migration scripts
- **Versioning**: Consider schema versioning for complex changes

### Testing Guidelines

#### Test Categories
- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test component interactions
- **Performance Tests**: Benchmark operations
- **Compatibility Tests**: Test with different Cassandra/Kafka versions

#### Test Naming
```java
@Test
void shouldValidateSchemaWithAllSupportedTypes()

@Test
void shouldThrowExceptionWhenSchemaIsInvalid()

@Test
void shouldHandleEdgeCaseWithNullValues()

@Test
void shouldPerformQueriesWithinTimeLimit()
```

#### Test Structure
```java
@DisplayName("Schema Validation")
class SchemaValidationTest {

    @Nested
    @DisplayName("Valid Schemas")
    class ValidSchemas {

        @Test
        @DisplayName("should accept schema with all supported column types")
        void shouldAcceptValidSchema() {
            // Test implementation
        }
    }

    @Nested
    @DisplayName("Invalid Schemas")
    class InvalidSchemas {

        @Test
        @DisplayName("should reject schema with duplicate column names")
        void shouldRejectDuplicateColumns() {
            // Test implementation
        }
    }
}
```

#### Test Data Guidelines
- **Realistic Data**: Use representative data sizes and patterns
- **Edge Cases**: Test null values, empty collections, special characters
- **Performance Data**: Use sufficient data for meaningful performance tests
- **Cleanup**: Ensure test data is properly cleaned up

### Performance Testing

#### Benchmark Categories
- **Query Performance**: Measure query latency and throughput
- **Memory Usage**: Monitor heap usage and garbage collection
- **CPU Utilization**: Track CPU usage during operations
- **Network Traffic**: Measure data transfer volumes

#### Benchmark Tools
```java
@Benchmark
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public void benchmarkTypedStoreQuery() {
    // Benchmark implementation
}

@Benchmark
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public void benchmarkStoreThroughput() {
    // Throughput benchmark
}
```

#### Performance Baselines
- **Latency Targets**: Query response time < 10ms for 95th percentile
- **Throughput Targets**: 1000+ operations/second
- **Memory Targets**: < 100MB heap usage for 10K records
- **Scalability**: Linear performance scaling with data size

### Code Review Guidelines

#### Review Checklist
- [ ] **Functionality**: Does the code work as intended?
- [ ] **Tests**: Are there sufficient tests? Do they pass?
- [ ] **Documentation**: Is the code well-documented?
- [ ] **Style**: Does it follow project conventions?
- [ ] **Performance**: Are there any performance concerns?
- [ ] **Security**: Are there any security implications?
- [ ] **Maintainability**: Is the code easy to understand and maintain?

#### Review Comments
- **Be specific**: Reference line numbers and suggest concrete fixes
- **Explain reasoning**: Why is this change needed?
- **Suggest alternatives**: If there are multiple approaches
- **Acknowledge good work**: Positive feedback is important
- **Mentor**: Help contributors learn and improve

#### Code Review Process
1. **Automated Checks**: Run CI/CD pipeline
2. **Self Review**: Author reviews their own code
3. **Peer Review**: At least one team member reviews
4. **Approval**: Required approvals before merge
5. **Merge**: Squash or rebase as appropriate

### Release Process

#### Version Numbering
- **Major**: Breaking changes (2.x.x)
- **Minor**: New features (x.3.x)
- **Patch**: Bug fixes (x.x.4)
- **Pre-release**: Alpha/beta/rc suffixes

#### Release Checklist
- [ ] All tests pass
- [ ] Documentation updated
- [ ] Performance benchmarks run
- [ ] Compatibility tested
- [ ] Security review completed
- [ ] Release notes written
- [ ] Artifacts published to Maven Central

#### Release Notes Format
```markdown
## [2.1.0] - 2024-01-15

### Added
- Schema-aware state stores with type safety
- Support for custom CQL types
- Performance improvements for interactive queries

### Changed
- Updated to Java 17 baseline
- Modernized API with records and text blocks

### Fixed
- Memory leak in validation logic
- Incorrect error messages in edge cases

### Performance
- 3.3x faster query performance
- 60% reduction in memory usage
```

## Styleguides
### Commit Messages
<!-- TODO

-->

## Join The Project Team
<!-- TODO -->

<!-- omit in toc -->
## Attribution
This guide is based on the **contributing-gen**. [Make your own](https://github.com/bttger/contributing-gen)!
