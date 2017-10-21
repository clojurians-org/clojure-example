goog.require('cljs.core');
cljs.core.enable_console_print_BANG_() ;
cljs.core.println(cljs.core.inc(1)) ;

var e=React.createElement ;
var rbButton=React.createFactory(ReactBootstrap.Button) ;
var rbForm = React.createFactory(ReactBootstrap.Form) ;
var rbFormGroup = React.createFactory(ReactBootstrap.FormGroup) ;
var rbInputGroup = React.createFactory(ReactBootstrap.InputGroup) ;
var rbInputGroupAddon =  React.createFactory(ReactBootstrap.InputGroup.Addon) ;
var rbFormControl =  React.createFactory(ReactBootstrap.FormControl) ;
var rAutocomplete=React.createFactory(ReactAutocomplete) ;

var clj=cljs.core.clj__GT_js ;
var println=cljs.core.println ;
var str=cljs.core.str ;
var range=cljs.core.range;
var map=cljs.core.map ;
var mapv=cljs.core.mapv ;
var map_indexed=cljs.core.map_indexed ;
var some=cljs.core.some ;

class formInstance extends React.Component {
  constructor(props) {
    super(props) ;
    this.state = {period: "", minute: "", hour: ""} ;
    this.periods=["ByMonth", "ByDay", "OneTime"] ;
    this.hours= clj(map(str, range(24))) ;
    this.minutes= clj(map(str, range(60))) ;
  }
  render() {
    return rbForm({inline: true}, [
      rbFormGroup({}, [
        rbInputGroup({}, [
          rbInputGroupAddon({}, ["Period"]),
          rAutocomplete({
            inputProps: {class: "form-control"},
            items: this.periods,
            getItemValue: item => item,
            shouldItemRender: (item, value) => item,
            renderItem: (item, isHighlighted) => rbButton({}, item),
            value: this.state.period,
            onChange: e => {},
            onSelect: val => this.setState({period: val}),
          })
        ])
      ]),
      "  ",
      rbFormGroup({}, [
        rbInputGroup({}, [
          rAutocomplete({
            inputProps: {class: "form-control"},
            items: this.hours,
            getItemValue: item => item,
            shouldItemRender: (item, value) => {
              return item.startsWith(value) ;
            },
            renderItem: (item, isHighlighted) => rbButton({}, item),
            value: this.state.hour,
            onChange: e => {
              var val = e.target.value ;
              if("" == val || null != some(it => it==val, this.hours)) {
                this.setState({hour: val}) ;
              }
            },
            onSelect: val => this.setState({hour: val}),
          }),
          rbInputGroupAddon({}, ["Hour"])
        ])
      ]),
      "  ",
      rbFormGroup({}, [
        rbInputGroup({}, [
          rAutocomplete({
            inputProps: {class: "form-control"},
            items: this.minutes,
            getItemValue: item => item,
            shouldItemRender: (item, value) => {
              return item.startsWith(value) ;
            },
            renderItem: (item, isHighlighted) => rbButton({}, item),
            value: this.state.minute,
            onChange: e => {
              var val = e.target.value ;
              if("" == val || null != some(it => it==val, this.minutes)) {
                this.setState({minute: val}) ;
              }
            },
            onSelect: val => this.setState({minute: val}),
          }),
          rbInputGroupAddon({}, ["Minute"])
        ])
      ])
    ]) 
  }
} ;
ReactDOM.render(e(formInstance), document.getElementById('app')) ;
