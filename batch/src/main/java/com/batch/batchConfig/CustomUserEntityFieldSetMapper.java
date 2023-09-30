package com.batch.batchConfig;

import com.batch.dao.UserEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

@Data
public class CustomUserEntityFieldSetMapper implements FieldSetMapper<UserEntity> {
    @Override
    public UserEntity mapFieldSet(FieldSet fieldSet) throws BindException {

        UserEntity user = new UserEntity();

        user.setFirstName(fieldSet.readString("First Name"));

        user.setLastName(fieldSet.readString("Last Name"));
        user.setGender(fieldSet.readString("Gender"));
        user.setCountry(fieldSet.readString("Country"));
        user.setAge(fieldSet.readString("Age"));
        user.setFirstName(fieldSet.readString("Created At"));

        return user;
    }
}
