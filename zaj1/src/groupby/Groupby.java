package groupby;

import dataframe.DataFrame;
import myExceptions.AddingWrongClassesException;
import myExceptions.WrongTypeInColumnException;

public interface Groupby {
    DataFrame max() throws WrongTypeInColumnException;
    DataFrame min() throws WrongTypeInColumnException;
    DataFrame mean() throws WrongTypeInColumnException;
    DataFrame std() throws WrongTypeInColumnException;
    DataFrame sum() throws WrongTypeInColumnException;
    DataFrame var() throws WrongTypeInColumnException;
    DataFrame apply(Applyable applyable);

}
