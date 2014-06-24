import java.io.Serializable;

public class CloudObject implements Serializable{

  public Integer click = 0;
  public Integer impression = 0;
  public Integer conversion = 0;
  public Integer win = 0;
  public Integer bid = 0;
  public Double cost = 0d;
  public Double revenue = 0d;

  public CloudObject(Integer click, Integer impression, Integer win, Integer bid, Double cost, Integer conversion, Double revenue) {
    this.click = click;
    this.impression = impression;
    this.cost = cost;
    this.conversion = conversion;
    this.revenue = revenue;
    this.win = win;
    this.bid = bid;
  }

  public CloudObject() {
  }

  public CloudObject(Integer impression) {
    this.impression = impression;
  }


  public CloudObject add(CloudObject cloudObject) {
    return new CloudObject(this.click + cloudObject.click, this.impression + cloudObject.impression, this.win + cloudObject.win, this.bid + cloudObject.bid, this.cost + cloudObject.cost, this.conversion + cloudObject.conversion, this.revenue + cloudObject.revenue);
  }

  @Override
  public String toString() {
    return "CloudObject{" + "click=" + click + ", impression=" + impression + ", win=" + win + ", bid=" + bid + ", conversion=" + conversion + ", cost=" + cost + ", revenue=" + revenue + '}';
  }
}
