package exec

import (
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

type Projection struct {
	*TaskBase
	sql *expr.SqlSelect
}

func NewProjection(sqlSelect *expr.SqlSelect) *Projection {
	s := &Projection{
		TaskBase: NewTaskBase("Projection"),
		sql:      sqlSelect,
	}
	if len(sqlSelect.From) > 1 {
		s.Handler = projectionUnAliasedEvaluator(sqlSelect, s)
	} else {
		s.Handler = projectionEvaluator(sqlSelect, s)
	}

	return s
}

// Create handler function for evaluation (ie, field selection from tuples)
func projectionUnAliasedEvaluator(sql *expr.SqlSelect, task TaskRunner) MessageHandler {
	out := task.MessageOut()
	//evaluator := vm.Evaluator(where)
	cols := sql.UnAliasedColumns()
	return func(ctx *Context, msg datasource.Message) bool {
		defer func() {
			if r := recover(); r != nil {
				u.Errorf("crap, %v", r)
			}
		}()

		var outMsg datasource.Message
		// uv := msg.Body().(url.Values)
		switch msgT := msg.(type) {
		case *datasource.UrlValuesMsg:
			mt := msg.Body().(*datasource.ContextUrlValues)
			// readContext := datasource.NewContextUrlValues(uv)
			// use our custom write context for example purposes
			writeContext := datasource.NewContextSimple()
			outMsg = writeContext
			//u.Infof("about to project: colsct%v %#v", len(sql.Columns), outMsg)
			for _, col := range cols {
				//u.Debugf("col:   %#v", col)
				if col.Guard != nil {
					ifColValue, ok := vm.Eval(mt, col.Guard)
					if !ok {
						u.Errorf("Could not evaluate if:   %v", col.Guard.StringAST())
						//return fmt.Errorf("Could not evaluate if clause: %v", col.Guard.String())
					}
					//u.Debugf("if eval val:  %T:%v", ifColValue, ifColValue)
					switch ifColVal := ifColValue.(type) {
					case value.BoolValue:
						if ifColVal.Val() == false {
							//u.Debugf("Filtering out col")
							continue
						}
					}
				}
				if col.Star {
					for k, v := range mt.Row() {
						writeContext.Put(&expr.Column{As: k}, nil, v)
					}
				} else {
					//u.Debugf("tree.Root: as?%v %#v", col.As, col.Expr)
					v, ok := vm.Eval(mt, col.Expr)
					//u.Debugf("evaled: ok?%v key=%v  val=%v", ok, col.Key(), v)
					if ok {
						writeContext.Put(col, mt, v)
					}
				}
			}

		case *datasource.SqlDriverMessageMap:
			// readContext := datasource.NewContextUrlValues(uv)
			// use our custom write context for example purposes
			writeContext := datasource.NewContextSimple()
			outMsg = writeContext
			//cols := sql.UnAliasedColumns()
			// vals := make([]driver.Value, len(cols))
			// for k, val := range mt.Vals {
			// 	if col, ok := cols[k]; ok {
			// 		vals[col.Index] = val
			// 	}
			// }
			//wrapMsg := datasource.NewValueContextWrapper(vals, cols)
			//u.Infof("about to project: colsct%v %#v", len(sql.Columns), outMsg)
			for _, col := range cols {
				//u.Debugf("col:   %#v", col)
				if col.Guard != nil {
					ifColValue, ok := vm.Eval(msgT, col.Guard)
					if !ok {
						u.Errorf("Could not evaluate if:   %v", col.Guard.StringAST())
						//return fmt.Errorf("Could not evaluate if clause: %v", col.Guard.String())
					}
					//u.Debugf("if eval val:  %T:%v", ifColValue, ifColValue)
					switch ifColVal := ifColValue.(type) {
					case value.BoolValue:
						if ifColVal.Val() == false {
							//u.Debugf("Filtering out col")
							continue
						}
					}
				}
				if col.Star {
					for k, v := range msgT.Row() {
						writeContext.Put(&expr.Column{As: k}, nil, v)
					}
				} else {
					_, key, _ := col.LeftRight()
					u.Debugf("tree.Root: as?%v right=%v   %v", col.As, key, col.Expr.StringAST())
					v, ok := vm.Eval(msgT, col.Expr)

					u.Debugf("evaled: ok?%v key=%v  val=%#v", ok, key, v)
					if ok {
						//writeContext.Put(col, msgT, v)
						writeContext.Data[key] = v
					}
				}

			}
		default:
			u.Warnf("got unrecognized msg: %#v", msg.Body())
		}

		//u.Debugf("completed projection for: %p %#v", out, outMsg)
		select {
		case out <- outMsg:
			return true
		case <-task.SigChan():
			return false
		}
	}
}

// Create handler function for evaluation (ie, field selection from tuples)
func projectionEvaluator(sql *expr.SqlSelect, task TaskRunner) MessageHandler {
	out := task.MessageOut()
	//evaluator := vm.Evaluator(where)
	return func(ctx *Context, msg datasource.Message) bool {
		defer func() {
			if r := recover(); r != nil {
				u.Errorf("crap, %v", r)
			}
		}()

		var outMsg datasource.Message
		// uv := msg.Body().(url.Values)
		switch msgT := msg.(type) {
		case *datasource.UrlValuesMsg:
			mt := msg.Body().(*datasource.ContextUrlValues)
			// readContext := datasource.NewContextUrlValues(uv)
			// use our custom write context for example purposes
			writeContext := datasource.NewContextSimple()
			outMsg = writeContext
			//u.Infof("about to project: colsct%v %#v", len(sql.Columns), outMsg)
			for _, col := range sql.Columns {
				//u.Debugf("col:   %#v", col)
				if col.Guard != nil {
					ifColValue, ok := vm.Eval(mt, col.Guard)
					if !ok {
						u.Errorf("Could not evaluate if:   %v", col.Guard.StringAST())
						//return fmt.Errorf("Could not evaluate if clause: %v", col.Guard.String())
					}
					//u.Debugf("if eval val:  %T:%v", ifColValue, ifColValue)
					switch ifColVal := ifColValue.(type) {
					case value.BoolValue:
						if ifColVal.Val() == false {
							//u.Debugf("Filtering out col")
							continue
						}
					}
				}
				if col.Star {
					for k, v := range mt.Row() {
						writeContext.Put(&expr.Column{As: k}, nil, v)
					}
				} else {
					//u.Debugf("tree.Root: as?%v %#v", col.As, col.Expr)
					v, ok := vm.Eval(mt, col.Expr)
					//u.Debugf("evaled: ok?%v key=%v  val=%v", ok, col.Key(), v)
					if ok {
						writeContext.Put(col, mt, v)
					}
				}
			}

		case *datasource.SqlDriverMessageMap:
			// readContext := datasource.NewContextUrlValues(uv)
			// use our custom write context for example purposes
			writeContext := datasource.NewContextSimple()
			outMsg = writeContext
			//cols := sql.UnAliasedColumns()
			// vals := make([]driver.Value, len(cols))
			// for k, val := range mt.Vals {
			// 	if col, ok := cols[k]; ok {
			// 		vals[col.Index] = val
			// 	}
			// }
			//wrapMsg := datasource.NewValueContextWrapper(vals, cols)
			//u.Infof("about to project: colsct%v %#v", len(sql.Columns), outMsg)
			for _, col := range sql.Columns {
				//u.Debugf("col:   %#v", col)
				if col.Guard != nil {
					ifColValue, ok := vm.Eval(msgT, col.Guard)
					if !ok {
						u.Errorf("Could not evaluate if:   %v", col.Guard.StringAST())
						//return fmt.Errorf("Could not evaluate if clause: %v", col.Guard.String())
					}
					//u.Debugf("if eval val:  %T:%v", ifColValue, ifColValue)
					switch ifColVal := ifColValue.(type) {
					case value.BoolValue:
						if ifColVal.Val() == false {
							//u.Debugf("Filtering out col")
							continue
						}
					}
				}
				if col.Star {
					for k, v := range msgT.Row() {
						writeContext.Put(&expr.Column{As: k}, nil, v)
					}
				} else {
					_, key, _ := col.LeftRight()
					u.Debugf("tree.Root: as?%v right=%v   %v", col.As, key, col.Expr.StringAST())
					v, ok := vm.Eval(msgT, col.Expr)

					u.Debugf("evaled: ok?%v key=%v  val=%#v", ok, key, v)
					if ok {
						//writeContext.Put(col, msgT, v)
						writeContext.Data[key] = v
					}
				}

			}
		default:
			u.Warnf("got unrecognized msg: %#v", msg.Body())
		}

		//u.Debugf("completed projection for: %p %#v", out, outMsg)
		select {
		case out <- outMsg:
			return true
		case <-task.SigChan():
			return false
		}
	}
}
