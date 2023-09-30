package com.batch.batchConfig;

import com.batch.dao.UserEntity;
import org.springframework.batch.item.ItemProcessor;

public class UserItemProcessor implements ItemProcessor<UserEntity, UserEntity> {

    @Override
    public UserEntity process(UserEntity user) throws Exception {
        return user;
    }
}
