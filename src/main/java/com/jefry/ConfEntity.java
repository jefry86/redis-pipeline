package com.jefry;

import lombok.Data;

import java.util.Map;


@Data
public class ConfEntity {
    private Map<String, String> Redis;
    private Map<String, String> File;
}
