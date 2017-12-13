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
import PropTypes, {object} from 'prop-types'

import ConfigEditUi from './ConfigEditUi'
import { fetchConfig } from '../../modules/config'

class ConfigEditForm extends Component {

  async componentDidMount () {
    const configId = this.context.router.route.match.params.configId
    this.context.store.dispatch(fetchConfig(configId))
  }

  render () {
    return (
      <ConfigEditUi />
    )
  }
}

ConfigEditForm.contextTypes = {
  store: PropTypes.object.isRequired,
  router: PropTypes.shape({
    history: object.isRequired
  })
}

export default ConfigEditForm
