package org.awsutils.dynamodb.config;




import jakarta.annotation.PostConstruct;
import org.awsutils.dynamodb.annotations.DDBTable;
import org.awsutils.dynamodb.exceptions.DbException;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.Constructor;
import java.text.MessageFormat;
import java.util.Set;

public class EntityValidationConfig {
    private final String dtoBasePackage;

    public EntityValidationConfig(final String dtoBasePackage) {
        this.dtoBasePackage = dtoBasePackage;
    }

    @PostConstruct
    public void validateEntities() {
        final Reflections reflections = new Reflections(new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage(
                dtoBasePackage, ClasspathHelper.contextClassLoader(),
                ClasspathHelper.staticClassLoader())));
        final Set<Class<?>> entityClasses = reflections.getTypesAnnotatedWith(DDBTable.class);

        entityClasses.forEach(this::validateEntityClass);
    }

    @SuppressWarnings("ConstantConditions")
    private void validateEntityClass(Class<?> entityClass) {
        try {
            final Constructor<?> constructor = entityClass.getDeclaredConstructor();

            if(constructor == null) {
                throw new NoSuchMethodException();
            }
        } catch (final NoSuchMethodException e) {
            throw new DbException(MessageFormat.format(
                    "Entity [{0}] does not have a default constructor. All entities should have at least one default no-args constructor",
                    entityClass.getName()));
        }
    }
}
