package com.yl.flink.table.my;

public class Tab1 {

    private int id;

    private String name;

    private int age;

    public Tab1() {
    }

    public Tab1(int id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Tab1{id:" + id +
                ",name=" + name +
                ",age=" + age +
                "}";
    }
}
