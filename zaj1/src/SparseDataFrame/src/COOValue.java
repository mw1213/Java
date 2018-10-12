package SparseDataFrame;

import dataframe.*;


public class COOValue extends Column {
	private Object[] values;
	private Object hide;
	int size;
	
	public COOValue (String _name, String _type, Object _hide){
		super (_name, _type);
		hide=_hide;
		size=0;

	}
	
    public void addElement(Object el){
        String el_type = el.getClass().toString();
        if(el_type.contains(this.type)) {
        	if (!el.equals(hide)){
        		list.add(el);
        		values[list.size()]=el;
        		size++;
        		
        	}
        	else{
        		size++;
        		values[size] = hide;
        	}
        	
        }
    }
    
    public int size(){
        return this.size;
    }
	
	
}
