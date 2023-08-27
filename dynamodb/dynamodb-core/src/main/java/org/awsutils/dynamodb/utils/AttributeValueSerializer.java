package org.awsutils.dynamodb.utils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructMap;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * <p>Title: AttributeValueSerializer</p>
 * <p>Description: Jackson JSON serializer for the DynamoDB V2 SDK's {@link AttributeValue}</p>
 * <p>Author: Whitehead</p>
 */
public class AttributeValueSerializer extends JsonSerializer<AttributeValue> {
    public static final DefaultSdkAutoConstructMap<?,?> EMPTY_ATTR_MAP = DefaultSdkAutoConstructMap.getInstance();
    public static final DefaultSdkAutoConstructList<?> EMPTY_ATTR_LIST = DefaultSdkAutoConstructList.getInstance();

    public static final JsonSerializer<AttributeValue> INSTANCE = new AttributeValueSerializer();

    public AttributeValueSerializer() {

    }

    @Override
    public void serialize(AttributeValue av, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (av == null) {
            gen.writeNull();
        } else {
            if (av.m() != EMPTY_ATTR_MAP) {
                gen.writeStartObject();
                Map<String, AttributeValue> map = av.m();
                for(Map.Entry<String, AttributeValue> entry : map.entrySet()) {
                    gen.writeFieldName(entry.getKey());
                    serialize(entry.getValue(), gen, serializers);
                }
                gen.writeEndObject();
            } else if (av.l() != EMPTY_ATTR_LIST) {
                List<AttributeValue> list = av.l();
                gen.writeStartArray();
                for(AttributeValue a : list) {
                    serialize(a, gen, serializers);
                }
                gen.writeEndArray();
            } else if (av.s() != null) {
                gen.writeString(av.s());
            } else if (av.n() != null) {
                gen.writeNumber(new BigDecimal(av.n()));
            } else if (av.bool() != null) {
                gen.writeBoolean(av.bool());
            } else if (av.nul() != null && av.nul()) {
                gen.writeNull();
            } else if (av.b() != null) {
                gen.writeBinary(av.b().asByteArray());
            } else if (av.ss() != EMPTY_ATTR_LIST) {
                List<String> list = av.ss();
                int size = list.size();
                gen.writeStartArray(size);
                for (String s : list) {
                    gen.writeString(s);
                }
                gen.writeEndArray();
            } else if (av.bs() != EMPTY_ATTR_LIST) {
                List<SdkBytes> list = av.bs();
                int size = list.size();
                gen.writeStartArray(size);
                for (SdkBytes sdkBytes : list) {
                    gen.writeBinary(sdkBytes.asByteArray());
                }
                gen.writeEndArray();
            } else if (av.ns() != EMPTY_ATTR_LIST) {
                List<String> list = av.ns();
                int size = list.size();
                gen.writeStartArray(size);
                for (String s : list) {
                    gen.writeNumber(new BigDecimal(s));
                }
                gen.writeEndArray();
            } else if (av.nul() != null) {
                gen.writeNull();
            }
//			else {
//				System.err.println("MISSED DATA TYPE: " + av);
//			}
        }
    }
}