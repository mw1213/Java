package groupby;

import dataframe.DataFrame;

public interface Groupby {
    DataFrame max();
    DataFrame min();
    DataFrame mean();
    DataFrame std();
    DataFrame sum();
    DataFrame var();
    DataFrame apply(Applyable applyable);

}
