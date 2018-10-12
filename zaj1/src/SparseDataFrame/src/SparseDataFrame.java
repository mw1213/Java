package SpraseDataFrame;

import dataframe.*;


public class SparseDataFrame extends DataFrame {
	
	private List<COOValue> columns
	
	public SparseDataFrame(String[] names, String[] types, Object _hide ){
		super(names, types);
		columns.hide = _hide;
		
	}
	
	public SparseDataFrame(DataFrame frame, Object hide){
		for (Columns c : frame){
			for(int i = 0 ; i<frame.size(); i++){
				if(!frame.get(i).equals(hide)){
					Object newValue = frame.columns.get(i);
					columns.values[i] = newValue;
					columns.size++;
				}
				else{
					columns.size++;
				}
			}
	}
	
	

	public DataFrame toDense(){
		DataFrame newDataFrame = new DataFrame(this.columns.name, this.columns.type);
		
        for (COOValue col : columns){
            for (int i =0; i<size(); i++)
            	
            }
        }
        
        return newDataframe;
	}
	

}
