package sockets;

public class VilleWithPrice {

    public String date ;
    public String ville;

    public double price;

    public VilleWithPrice() {}

    public VilleWithPrice(String ville, double price) {
        this.ville = ville;
        this.price = price;
    }

    @Override
    public String toString() {
        return ville + " : " + price;
    }
}
