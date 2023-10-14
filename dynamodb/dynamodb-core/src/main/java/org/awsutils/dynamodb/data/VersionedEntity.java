package org.awsutils.dynamodb.data;


import org.awsutils.dynamodb.annotations.DbAttribute;
import org.awsutils.dynamodb.annotations.VersionAttribute;

public class VersionedEntity {
    @VersionAttribute
    @DbAttribute("version")
    private Integer version;

    public Integer getVersion() {
        return version;
    }

    public void setVersion(final Integer version) {
        this.version = version;
    }
}
