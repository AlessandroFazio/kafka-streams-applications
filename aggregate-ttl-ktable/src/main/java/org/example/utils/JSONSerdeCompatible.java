package org.example.utils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.example.wrapper.AggregateObject;
import org.example.wrapper.ValueWrapper;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = AggregateObject.class, name = "aggregateObject"),
        @JsonSubTypes.Type(value = ValueWrapper.class, name = "valueWrapper"),
})
public abstract class JSONSerdeCompatible {
}
