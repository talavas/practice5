package shpp.level3.util;

public enum DBReferences {
    PRODUCTS("products"),
    INVENTORY("inventory"),
    PRODUCT_TYPES("product-types"),
    STORES("stores"),
    INVENTORY_BY_TYPE("inventory-by-type");
    private final  String name;

    DBReferences(String name){
        this.name = name;
    }

    public String getName(){
        return name;
    }
}
