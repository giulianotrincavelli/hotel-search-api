package com.sling.hotel.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

class HexagonalArchitectureTest {

    private static final String BASE = "com.sling.hotel";
    private static final String DOMAIN = BASE + ".domain..";
    private static final String APPLICATION = BASE + ".application..";
    private static final String INFRASTRUCTURE = BASE + ".infrastructure..";
    private static final String ADAPTERS_IN = BASE + ".infrastructure.adapter.in..";
    private static final String ADAPTERS_OUT = BASE + ".infrastructure.adapter.out..";
    private static final String USE_CASES = BASE + ".application.usecase..";
    private static final String PORTS = BASE + ".application.ports..";

    private static JavaClasses classes;

    @BeforeAll
    static void importClasses() {
        classes = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages(BASE);
    }

    @Test
    void domainShouldNotDependOnApplication() {
        noClasses().that().resideInAPackage(DOMAIN)
                .should().dependOnClassesThat().resideInAPackage(APPLICATION)
                .because("Domain must not depend on Application")
                .check(classes);
    }

    @Test
    void domainShouldNotDependOnInfrastructure() {
        noClasses().that().resideInAPackage(DOMAIN)
                .should().dependOnClassesThat().resideInAPackage(INFRASTRUCTURE)
                .because("Domain must not depend on Infrastructure")
                .check(classes);
    }

    @Test
    void applicationShouldNotDependOnInfrastructure() {
        noClasses().that().resideInAPackage(APPLICATION)
                .should().dependOnClassesThat().resideInAPackage(INFRASTRUCTURE)
                .because("Application must not depend on Infrastructure")
                .check(classes);
    }

    @Test
    void domainShouldNotDependOnSpring() {
        noClasses().that().resideInAPackage(DOMAIN)
                .should().dependOnClassesThat().resideInAPackage("org.springframework..")
                .because("Domain must not depend on Spring")
                .check(classes);
    }

    @Test
    void applicationShouldNotDependOnSpring() {
        noClasses().that().resideInAPackage(APPLICATION)
                .should().dependOnClassesThat().resideInAPackage("org.springframework..")
                .because("Application must not depend on Spring")
                .check(classes);
    }

    @Test
    void domainShouldNotDependOnKafka() {
        noClasses().that().resideInAPackage(DOMAIN)
                .should().dependOnClassesThat().resideInAPackage("org.apache.kafka..")
                .because("Domain must not depend on Kafka")
                .check(classes);
    }

    @Test
    void applicationShouldNotDependOnKafka() {
        noClasses().that().resideInAPackage(APPLICATION)
                .should().dependOnClassesThat().resideInAPackage("org.apache.kafka..")
                .because("Application must not depend on Kafka")
                .check(classes);
    }

    @Test
    void domainShouldNotDependOnJpa() {
        noClasses().that().resideInAPackage(DOMAIN)
                .should().dependOnClassesThat().resideInAPackage("jakarta.persistence..")
                .because("Domain must not depend on JPA")
                .check(classes);
    }

    @Test
    void applicationShouldNotDependOnJpa() {
        noClasses().that().resideInAPackage(APPLICATION)
                .should().dependOnClassesThat().resideInAPackage("jakarta.persistence..")
                .because("Application must not depend on JPA")
                .check(classes);
    }

    @Test
    void inboundAdaptersShouldNotDependOnOutboundAdapters() {
        noClasses().that().resideInAPackage(ADAPTERS_IN)
                .should().dependOnClassesThat().resideInAPackage(ADAPTERS_OUT)
                .because("Inbound adapters must not depend on outbound adapters")
                .check(classes);
    }

    @Test
    void outboundAdaptersShouldNotDependOnInboundAdapters() {
        noClasses().that().resideInAPackage(ADAPTERS_OUT)
                .should().dependOnClassesThat().resideInAPackage(ADAPTERS_IN)
                .because("Outbound adapters must not depend on inbound adapters")
                .check(classes);
    }

    @Test
    void useCasesShouldResideInApplicationLayer() {
        classes().that().haveSimpleNameEndingWith("UseCase")
                .should().resideInAPackage(USE_CASES)
                .because("Use case implementations must reside in the application layer")
                .check(classes);
    }

    @Test
    void portsShouldResideInApplicationLayer() {
        classes().that().haveSimpleNameEndingWith("Port")
                .should().resideInAPackage(PORTS)
                .because("Port interfaces must reside in the application layer")
                .check(classes);
    }
}
