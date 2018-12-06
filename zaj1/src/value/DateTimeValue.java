package value;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.Locale;

public class DateTimeValue extends Value {
    private Date date;
    private boolean set;

    public DateTimeValue (){
        this.date=new Date();
        set=true;
    }
    public DateTimeValue(Date date) {
        this.date = date;
        set=true;
    }

    public DateTimeValue (String s) throws ParseException {
        try{
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
            date = formatter.parse(s);
            set = true;
        } catch (Exception e){
            set =false;
        }
    }

    @Override
    public boolean getSet(){
        return set;
    }

    @Override
    public String toString() {
        DateFormat dt1 = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        String requiredDate = dt1.format(date).toString();
        return requiredDate;
    }

    @Override
    public Value add(Value o1) {
        if(o1 instanceof DateTimeValue) {
            Date valueDate = (Date) o1.getValue();
            return new DateTimeValue(new Date(this.date.getYear(),
                    this.date.getMonth(),
                    this.date.getDay(),
                    this.date.getHours() + valueDate.getHours(),
                    this.date.getMinutes() + valueDate.getMinutes()));
        }
        throw new IllegalArgumentException();
    }

    @Override
    public Value sub(Value o1) {
        if(o1 instanceof DateTimeValue) {
            Date valueDate = (Date) o1.getValue();
            return new DateTimeValue(new Date(this.date.getYear(),
                    this.date.getMonth(),
                    this.date.getDay(),
                    this.date.getHours() + valueDate.getHours(),
                    this.date.getMinutes() + valueDate.getMinutes()));
        }
        throw new IllegalArgumentException();
    }

    @Override
    public Value mul(Value o1) {
        return new DateTimeValue(this.date);
    }

    @Override
    public Value div(Value o1) {
        return new DateTimeValue(this.date);
    }

    @Override
    public Value pow(Value o1) {
        return new DateTimeValue(this.date);
    }

    @Override
    public boolean eq(Value o1) {
        if(o1 instanceof DateTimeValue) {
            return this.date.equals((Date) o1.getValue());
        }
        return false;
    }

    @Override
    public boolean lte(Value o1) {
        if(o1 instanceof DateTimeValue) {
            return this.date.compareTo((Date) o1.getValue()) < 0;
        }
        return false;
    }

    @Override
    public boolean gte(Value o1) {
        if(o1 instanceof DateTimeValue) {
            return this.date.compareTo((Date) o1.getValue()) > 0;
        }
        return false;
    }

    @Override
    public boolean neq(Value o1) {
        if(o1 instanceof DateTimeValue) {
            return !this.date.equals((Date) o1.getValue());
        }
        return false;
    }

    @Override
    public boolean equals(Object other) {
        if (this.hashCode() == other.hashCode()) return true;
        return false;
    }

    @Override
    public int hashCode() {
        return this.date.hashCode();
    }

    @Override
    public Value create(String s) {
        return new DateTimeValue(new Date(Date.parse(s)));
    }

    @Override
    public Object getValue() {
        return date;
    }
}