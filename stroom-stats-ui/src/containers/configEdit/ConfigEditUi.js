/*
 * Copyright 2017 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React, {Component} from 'react'
import { connect } from 'react-redux'
import { bindActionCreators } from 'redux'
import { reduxForm } from 'redux-form'

//import './ConfigEdit.css'
import {saveConfig as onSubmit} from '../../modules/config'

export class ConfigEditUi extends Component {

  render () {
      const { handleSubmit, pristine, submitting, form } = this.props

    return (
            <div>Todo: Form for editing </div>
    )
  }
}

const ReduxConfigEditUi = reduxForm({
  form: 'ConfigEditForm'
})(ConfigEditUi)

const mapStateToProps = state => ({
})

const mapDispatchToProps = dispatch => bindActionCreators({
  onSubmit
}, dispatch)

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(ReduxConfigEditUi)
