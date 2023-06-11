package shpp.level3.util;

public enum DBReferences {
    PRODUCTS("products"),//scheme path = products/productUid/Product object
    INVENTORY("inventory"),//scheme path = inventory/productTypeUid/storeUid/productUid/quantity
    PRODUCT_TYPES("product-types"),//scheme path = products-types/productTypeUid/name
    STORES("stores"),//scheme path = stores/storeUid/Store object
    INVENTORY_BY_TYPE("inventory-by-type");// scheme path inventory-by-type/stores/storeUid/quantity value
    private final  String name;

    DBReferences(String name){
        this.name = name;
    }

    public String getName(){
        return name;
    }
}
