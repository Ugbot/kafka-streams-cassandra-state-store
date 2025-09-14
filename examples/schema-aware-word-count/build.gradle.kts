plugins {
    id("java")
    id("application")
}

group = "dev.thriving.oss.kafka.streams.cassandra.state.store"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":kafka-streams-cassandra-state-store"))

    // Kafka Streams
    implementation("org.apache.kafka:kafka-streams:3.6.0")

    // Cassandra driver
    implementation("com.datastax.oss:java-driver-core:4.17.0")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.7")
    implementation("ch.qos.logback:logback-classic:1.4.11")

    // Test dependencies
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
    testImplementation("org.assertj:assertj-core:3.24.2")
    testImplementation("org.testcontainers:junit-jupiter:1.18.3")
    testImplementation("org.testcontainers:cassandra:1.18.3")
    testImplementation("org.testcontainers:kafka:1.18.3")
}

application {
    mainClass.set("dev.thriving.oss.kafka.streams.cassandra.state.store.example.SchemaAwareWordCount")
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.test {
    useJUnitPlatform()
}
